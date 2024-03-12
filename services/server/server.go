/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"expvar"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	csapi "github.com/containerd/containerd/api/services/content/v1"
	ssapi "github.com/containerd/containerd/api/services/snapshots/v1"
	"github.com/containerd/containerd/content/local"
	csproxy "github.com/containerd/containerd/content/proxy"
	"github.com/containerd/containerd/defaults"
	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/events/exchange"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/pkg/dialer"
	"github.com/containerd/containerd/pkg/timeout"
	"github.com/containerd/containerd/plugin"
	srvconfig "github.com/containerd/containerd/services/server/config"
	ssproxy "github.com/containerd/containerd/snapshots/proxy"
	"github.com/containerd/containerd/sys"
	"github.com/containerd/ttrpc"
	"github.com/docker/go-metrics"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// CreateTopLevelDirectories creates the top-level root and state directories.
// 校验root、state目录必须存在，而且必须配置合适的权限
func CreateTopLevelDirectories(config *srvconfig.Config) error {
	switch {
	case config.Root == "":
		return errors.New("root must be specified")
	case config.State == "":
		return errors.New("state must be specified")
	case config.Root == config.State:
		return errors.New("root and state must be different paths")
	}

	if err := sys.MkdirAllWithACL(config.Root, 0711); err != nil {
		return err
	}

	if err := sys.MkdirAllWithACL(config.State, 0711); err != nil {
		return err
	}

	if config.TempDir != "" {
		if err := sys.MkdirAllWithACL(config.TempDir, 0711); err != nil {
			return err
		}
		if runtime.GOOS == "windows" {
			// On Windows, the Host Compute Service (vmcompute) will read the
			// TEMP/TMP setting from the calling process when creating the
			// tempdir to extract an image layer to. This allows the
			// administrator to align the tempdir location with the same volume
			// as the snapshot dir to avoid a copy operation when moving the
			// extracted layer to the snapshot dir location.
			os.Setenv("TEMP", config.TempDir)
			os.Setenv("TMP", config.TempDir)
		} else {
			os.Setenv("TMPDIR", config.TempDir)
		}
	}
	return nil
}

// New creates and initializes a new containerd server
// 1、设置OOM Score以及cGroup
// 2、保存全局的超时时间，主要有io.containerd.timeout.bolt.open=0s, io.containerd.timeout.metrics.shimstats=2s,
// io.containerd.timeout.shim.cleanup=5s, io.containerd.timeout.shim.load=5s, io.containerd.timeout.shim.shutdown=3s,
// io.containerd.timeout.task.state=2s
// 3、加载插件：
// 3.1、加载用户自定义插件，不过这个功能暂时还不支持，需要等到containerd 1.8版本以上时才支持
// 3.2、注册ContentPlugin TODO 需要仔细分析ContentPlugin插件的具体作用
// 3.3、注册ProxyPlugin插件
// 3.4、把注册上来的插件按照依赖关系进行排序，被依赖的插件需要排在前面，后面初始化的时候就会先初始化
// 3.5、需要注意的是，这里仅仅是插件的注册，并非是插件初始化
// 3.6、实际上虽然这里仅仅只注册了ContentPlugin以及ProxyPlugin这两种插件，但是由于register是一个全局的注册中心，很多插件都是直接
// 在自己的init()函数当中通过plugin.Register()方法注册了自己，因此这里plugin.Graph()函数返回的是所有的插件，因为init函数肯定要
// 比当前方法执行靠前。
// 4、注册流处理器（StreamProcessor） TODO 需要仔细分析
// 5、设置grpc参数
func New(
	ctx context.Context, //上下文参数，携带有某些信息
	config *srvconfig.Config, //用户为containerd设置的配置文件
) (*Server, error) {
	// 主要是为了设置OOM参数以及Cgroup
	if err := apply(ctx, config); err != nil {
		return nil, err
	}
	// 设置超时参数，这里使用一个Map来保存，containerd默认设置的参数如下：
	/*
	  "io.containerd.timeout.bolt.open" = "0s"
	  "io.containerd.timeout.metrics.shimstats" = "2s"
	  "io.containerd.timeout.shim.cleanup" = "5s"
	  "io.containerd.timeout.shim.load" = "5s"
	  "io.containerd.timeout.shim.shutdown" = "3s"
	  "io.containerd.timeout.task.state" = "2s"
	*/
	for key, sec := range config.Timeouts {
		d, err := time.ParseDuration(sec)
		if err != nil {
			return nil, fmt.Errorf("unable to parse %s into a time duration", sec)
		}
		timeout.Set(key, d)
	}
	// 加载插件：
	// 1、加载用户自定义插件，不过这个功能暂时还不支持，需要等到containerd 1.8版本以上时才支持
	// 2、注册ContentPlugin TODO 需要仔细分析ContentPlugin插件的具体作用
	// 3、注册ProxyPlugin插件
	// 4、把注册上来的插件按照依赖关系进行排序，被依赖的插件需要排在前面，后面初始化的时候就会先初始化
	// 5、需要注意的是，这里仅仅是插件的注册，并非是插件初始化
	// 6、实际上虽然这里仅仅只注册了ContentPlugin以及ProxyPlugin这两种插件，但是由于register是一个全局的注册中心，很多插件都是直接
	// 在自己的init()函数当中通过plugin.Register()方法注册了自己，因此这里plugin.Graph()函数返回的是所有的插件，因为init函数肯定要
	// 比当前方法执行靠前。
	plugins, err := LoadPlugins(ctx, config)
	if err != nil {
		return nil, err
	}
	// TODO StreamProcessor是啥玩意？
	for id, p := range config.StreamProcessors {
		diff.RegisterProcessor(diff.BinaryHandler(id, p.Returns, p.Accepts, p.Path, p.Args, p.Env))
	}

	// TODO 增加了GRPC Server Option参数
	serverOpts := []grpc.ServerOption{
		// 流拦截器
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			otelgrpc.StreamServerInterceptor(),
			grpc_prometheus.StreamServerInterceptor,
			streamNamespaceInterceptor,
		)),
		// 一元拦截器
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			otelgrpc.UnaryServerInterceptor(),
			grpc_prometheus.UnaryServerInterceptor,
			unaryNamespaceInterceptor,
		)),
	}
	// 设置GRPC可以消息的最大阈值，目前默认设置的是16MB
	if config.GRPC.MaxRecvMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(config.GRPC.MaxRecvMsgSize))
	}
	// 设置GRPC发送消息的最大阈值，目前最大设置的是16MB
	if config.GRPC.MaxSendMsgSize > 0 {
		serverOpts = append(serverOpts, grpc.MaxSendMsgSize(config.GRPC.MaxSendMsgSize))
	}
	// 实例化TTRPCServer，所谓的TTRPC，实际上就设置GRPC over TLS
	ttrpcServer, err := newTTRPCServer()
	if err != nil {
		return nil, err
	}
	tcpServerOpts := serverOpts
	// 设置TLS证书
	if config.GRPC.TCPTLSCert != "" {
		log.G(ctx).Info("setting up tls on tcp GRPC services...")

		tlsCert, err := tls.LoadX509KeyPair(config.GRPC.TCPTLSCert, config.GRPC.TCPTLSKey)
		if err != nil {
			return nil, err
		}
		tlsConfig := &tls.Config{Certificates: []tls.Certificate{tlsCert}}

		if config.GRPC.TCPTLSCA != "" {
			caCertPool := x509.NewCertPool()
			caCert, err := os.ReadFile(config.GRPC.TCPTLSCA)
			if err != nil {
				return nil, fmt.Errorf("failed to load CA file: %w", err)
			}
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.ClientCAs = caCertPool
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}

		tcpServerOpts = append(tcpServerOpts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	// grpcService allows GRPC services to be registered with the underlying server
	// containerd的插件一般都是至少实现了grpc, tcp, ttrpc这三个服务中的一个或者多个
	type grpcService interface {
		Register(*grpc.Server) error
	}

	// tcpService allows GRPC services to be registered with the underlying tcp server
	type tcpService interface {
		RegisterTCP(*grpc.Server) error
	}

	// ttrpcService allows TTRPC services to be registered with the underlying server
	type ttrpcService interface {
		RegisterTTRPC(*ttrpc.Server) error
	}

	var (
		// 实例化GRPC Server
		grpcServer = grpc.NewServer(serverOpts...)
		// 实例化TCP Server
		tcpServer = grpc.NewServer(tcpServerOpts...)

		grpcServices  []grpcService  // 用于保存当前实现了grpc服务的插件，其实就可以把当前插件看成一个grpc服务
		tcpServices   []tcpService   // 用于保存当前实现了tcp服务的插件，其实就可以把当前插件看成一个tcp服务
		ttrpcServices []ttrpcService // 用于保存当前实现了ttrpc服务的插件，其实就可以把当前插件看成一个ttrpc服务

		s = &Server{
			grpcServer:  grpcServer,
			tcpServer:   tcpServer,
			ttrpcServer: ttrpcServer,
			config:      config,
		}
		// TODO: Remove this in 2.0 and let event plugin crease it
		events      = exchange.NewExchange()
		initialized = plugin.NewPluginSet() // 用于保存已经注册好的插件
		required    = make(map[string]struct{})
	)
	// 遍历所有需要加载的插件，使用Map来保存，这样就可以通过O(1)的空间复杂度遍历需要的插件，加载containerd的初始化
	for _, r := range config.RequiredPlugins {
		required[r] = struct{}{}
	}
	// 1、遍历插件注册信息，根据插件的配置实例化每一个插件
	// 2、需要注意的是，这里的plugins其实是containerd的所有插件，他们大多数都是通过自己的init()方法将自己注册到了register注册中心当中的。
	for _, p := range plugins {
		id := p.URI()
		reqID := id
		if config.GetVersion() == 1 {
			reqID = p.ID
		}
		log.G(ctx).WithField("type", p.Type).Infof("loading plugin %q...", id)

		// 实例化当前插件的初始化上下文
		// 注意，这里修改了每个插件的root目录，规则为：<root>/<plugin-type>.<plugin-id>
		initContext := plugin.NewContext(
			ctx,          // 当前上下文呢
			p,            // 当前插件
			initialized,  // 已经初始化好的插件
			config.Root,  // /var/lib/containerd
			config.State, // /run/containerd，也就是保存socket的位置
		)

		// 继续初始化插件上下文
		initContext.Events = events
		initContext.Address = config.GRPC.Address
		initContext.TTRPCAddress = config.TTRPC.Address
		initContext.RegisterReadiness = s.RegisterReadiness

		// 加载插件配置
		if p.Config != nil {
			// 反序列化当前插件的配置
			pc, err := config.Decode(p)
			if err != nil {
				return nil, err
			}
			// 初始化插件配置
			initContext.Config = pc
		}
		// 执行插件的InitFn函数，实例化插件实体
		result := p.Init(initContext)
		// 保存实例化好的插件实体，与此同时会检查插件是否重复注册
		if err := initialized.Add(result); err != nil {
			return nil, fmt.Errorf("could not add plugin result to plugin set: %w", err)
		}

		// 获取实例化的插件实体，并且获取实例化插件实体时的错误
		instance, err := result.Instance()
		if err != nil {
			if plugin.IsSkipPlugin(err) {
				log.G(ctx).WithError(err).WithField("type", p.Type).Infof("skip loading plugin %q...", id)
			} else {
				log.G(ctx).WithError(err).Warnf("failed to load plugin %s", id)
			}

			// 如果当前插件实例化失败，但是该插件又是必须要加载的，那么直接返回错误，因为containerd很有可能不会按照用户的预期运行
			if _, ok := required[reqID]; ok {
				return nil, fmt.Errorf("load required plugin %s: %w", id, err)
			}
			continue
		}

		// 每成功实例化一个插件，都需要从required中删除此插件，用于标记当前插件已经初始化完成，已经满足要求
		delete(required, reqID)
		// check for grpc services that should be registered with the server
		// 也就是说插件要么是一个GRPCService，要么是TTRPCService, 要么是TCPService，也有可能实现了多个
		if src, ok := instance.(grpcService); ok {
			grpcServices = append(grpcServices, src)
		}
		if src, ok := instance.(ttrpcService); ok {
			ttrpcServices = append(ttrpcServices, src)
		}
		if service, ok := instance.(tcpService); ok {
			tcpServices = append(tcpServices, service)
		}

		s.plugins = append(s.plugins, result)
	}
	// 如果插件加载完成，但是还有必要的插件没有加载，那么只能退出containerd的初始化
	if len(required) != 0 {
		var missing []string
		for id := range required {
			missing = append(missing, id)
		}
		return nil, fmt.Errorf("required plugin %s not included", missing)
	}

	// register services after all plugins have been initialized
	// 注册服务，本质上注册的其实是插件的能力
	for _, service := range grpcServices {
		if err := service.Register(grpcServer); err != nil {
			return nil, err
		}
	}
	for _, service := range ttrpcServices {
		if err := service.RegisterTTRPC(ttrpcServer); err != nil {
			return nil, err
		}
	}
	for _, service := range tcpServices {
		if err := service.RegisterTCP(tcpServer); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// Server is the containerd main daemon
// Server实际上就是containerd后台进程的抽象
type Server struct {
	grpcServer  *grpc.Server      // grpc服务，一个containerd插件一般都是实现grpc, ttrpc, tcp服务中的一个或者多个
	ttrpcServer *ttrpc.Server     // grpc over tls服务
	tcpServer   *grpc.Server      // tcp服务
	config      *srvconfig.Config // containerd的配置
	plugins     []*plugin.Plugin  // 注册成功的插件
	ready       sync.WaitGroup    // 用于等待Server启动完成
}

// ServeGRPC provides the containerd grpc APIs on the provided listener
func (s *Server) ServeGRPC(l net.Listener) error {
	if s.config.Metrics.GRPCHistogram {
		// enable grpc time histograms to measure rpc latencies
		grpc_prometheus.EnableHandlingTimeHistogram()
	}
	// before we start serving the grpc API register the grpc_prometheus metrics
	// handler.  This needs to be the last service registered so that it can collect
	// metrics for every other service
	grpc_prometheus.Register(s.grpcServer)
	return trapClosedConnErr(s.grpcServer.Serve(l))
}

// ServeTTRPC provides the containerd ttrpc APIs on the provided listener
func (s *Server) ServeTTRPC(l net.Listener) error {
	return trapClosedConnErr(s.ttrpcServer.Serve(context.Background(), l))
}

// ServeMetrics provides a prometheus endpoint for exposing metrics
func (s *Server) ServeMetrics(l net.Listener) error {
	m := http.NewServeMux()
	m.Handle("/v1/metrics", metrics.Handler())
	srv := &http.Server{
		Handler:           m,
		ReadHeaderTimeout: 5 * time.Minute, // "G112: Potential Slowloris Attack (gosec)"; not a real concern for our use, so setting a long timeout.
	}
	return trapClosedConnErr(srv.Serve(l))
}

// ServeTCP allows services to serve over tcp
func (s *Server) ServeTCP(l net.Listener) error {
	grpc_prometheus.Register(s.tcpServer)
	return trapClosedConnErr(s.tcpServer.Serve(l))
}

// ServeDebug provides a debug endpoint
func (s *Server) ServeDebug(l net.Listener) error {
	// don't use the default http server mux to make sure nothing gets registered
	// that we don't want to expose via containerd
	m := http.NewServeMux()
	m.Handle("/debug/vars", expvar.Handler())
	m.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	m.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
	m.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	m.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	m.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	srv := &http.Server{
		Handler:           m,
		ReadHeaderTimeout: 5 * time.Minute, // "G112: Potential Slowloris Attack (gosec)"; not a real concern for our use, so setting a long timeout.
	}
	return trapClosedConnErr(srv.Serve(l))
}

// Stop the containerd server canceling any open connections
func (s *Server) Stop() {
	s.grpcServer.Stop()
	for i := len(s.plugins) - 1; i >= 0; i-- {
		p := s.plugins[i]
		instance, err := p.Instance()
		if err != nil {
			log.L.WithError(err).WithField("id", p.Registration.URI()).
				Error("could not get plugin instance")
			continue
		}
		closer, ok := instance.(io.Closer)
		if !ok {
			continue
		}
		if err := closer.Close(); err != nil {
			log.L.WithError(err).WithField("id", p.Registration.URI()).
				Error("failed to close plugin")
		}
	}
}

// RegisterReadiness 用于判断注册是否就绪
func (s *Server) RegisterReadiness() func() {
	s.ready.Add(1)
	return func() {
		s.ready.Done()
	}
}

func (s *Server) Wait() {
	s.ready.Wait()
}

// LoadPlugins loads all plugins into containerd and generates an ordered graph
// of all plugins.
// 1、加载用户自定义插件，不过这个功能暂时还不支持，需要等到containerd 1.8版本以上时才支持
// 2、注册ContentPlugin TODO 需要仔细分析ContentPlugin插件的具体作用
// 3、注册ProxyPlugin插件
// 4、把注册上来的插件按照依赖关系进行排序，被依赖的插件需要排在前面，后面初始化的时候就会先初始化
// 5、实际上虽然这里仅仅只注册了ContentPlugin以及ProxyPlugin这两种插件，但是由于register是一个全局的注册中心，很多插件都是直接
// 在自己的init()函数当中通过plugin.Register()方法注册了自己，因此这里plugin.Graph()函数返回的是所有的插件，因为init函数肯定要
// 比当前方法执行靠前。
func LoadPlugins(ctx context.Context, config *srvconfig.Config) ([]*plugin.Registration, error) {
	// load all plugins into containerd
	// 如果没有指定插件的位置，那么默认从/var/lib/containerd/plugins目录中加载插件
	path := config.PluginDir
	if path == "" {
		path = filepath.Join(config.Root, "plugins")
	}
	// 实际上这里目前是空的，并不会加载任何插件，containerd需要再1.8以上的版本才会支持
	if err := plugin.Load(path); err != nil {
		return nil, err
	}
	// load additional plugins that don't automatically register themselves
	// TODO content插件究竟干了啥？
	// TODO 这个插件和content-service插件有何区别？
	plugin.Register(&plugin.Registration{
		Type: plugin.ContentPlugin,
		ID:   "content",
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			ic.Meta.Exports["root"] = ic.Root
			// 注意，每个插件在初始化的时候都被修改了root目录，规则为：<root>/<plugin-type>.<plugin-id>
			// 对于content插件来说，root目录为：/var/lib/containerd/io.containerd.content.v1.content
			// TODO 如何理解这个插件的工作职责？
			return local.NewStore(ic.Root)
		},
	})

	// TODO 什么叫做代理客户端？
	clients := &proxyClients{}
	// TODO 代理插件干嘛用的？
	for name, proxyPlugin := range config.ProxyPlugins {
		var (
			t plugin.Type
			f func(*grpc.ClientConn) interface{}

			address = proxyPlugin.Address
		)

		// TODO 为什么代理插件只支持snapshot, content类型插件？
		switch proxyPlugin.Type {
		case plugin.SnapshotPlugin, "snapshot":
			t = plugin.SnapshotPlugin
			ssname := name
			f = func(conn *grpc.ClientConn) interface{} {
				return ssproxy.NewSnapshotter(ssapi.NewSnapshotsClient(conn), ssname)
			}

		case plugin.ContentPlugin, "content":
			t = plugin.ContentPlugin
			f = func(conn *grpc.ClientConn) interface{} {
				return csproxy.NewContentStore(csapi.NewContentClient(conn))
			}
		default:
			log.G(ctx).WithField("type", proxyPlugin.Type).Warn("unknown proxy plugin type")
		}

		// 注册代理插件
		plugin.Register(&plugin.Registration{
			Type: t,
			ID:   name,
			InitFn: func(ic *plugin.InitContext) (interface{}, error) {
				// 对外暴露插件的地址
				ic.Meta.Exports["address"] = address
				// 获取客户端，该客户端后面会和代理插件交互
				conn, err := clients.getClient(address)
				if err != nil {
					return nil, err
				}
				return f(conn), nil
			},
		})

	}

	// 目前一般都是使用的V2类型
	filter := srvconfig.V2DisabledFilter
	if config.GetVersion() == 1 {
		filter = srvconfig.V1DisabledFilter
	}
	// return the ordered graph for plugins
	// 所谓的插件图，其实就是由于插件的依赖关系导致的。有些插件可能依赖另外插件的能力，因此这些被依赖的插件肯定就需要先运行，以来插件就需要后启动
	return plugin.Graph(filter(config.DisabledPlugins)), nil
}

type proxyClients struct {
	m       sync.Mutex
	clients map[string]*grpc.ClientConn
}

func (pc *proxyClients) getClient(address string) (*grpc.ClientConn, error) {
	pc.m.Lock()
	defer pc.m.Unlock()
	if pc.clients == nil {
		pc.clients = map[string]*grpc.ClientConn{}
	} else if c, ok := pc.clients[address]; ok {
		return c, nil
	}

	backoffConfig := backoff.DefaultConfig
	backoffConfig.MaxDelay = 3 * time.Second
	connParams := grpc.ConnectParams{
		Backoff: backoffConfig,
	}
	gopts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(connParams),
		grpc.WithContextDialer(dialer.ContextDialer),

		// TODO(stevvooe): We may need to allow configuration of this on the client.
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(defaults.DefaultMaxRecvMsgSize)),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(defaults.DefaultMaxSendMsgSize)),
	}

	conn, err := grpc.Dial(dialer.DialAddress(address), gopts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %q: %w", address, err)
	}

	pc.clients[address] = conn

	return conn, nil
}

func trapClosedConnErr(err error) error {
	if err == nil || errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}
