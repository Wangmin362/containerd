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

package command

import (
	gocontext "context"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	"github.com/containerd/containerd/defaults"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	_ "github.com/containerd/containerd/metrics" // import containerd build info
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/services/server"
	srvconfig "github.com/containerd/containerd/services/server/config"
	"github.com/containerd/containerd/sys"
	"github.com/containerd/containerd/version"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc/grpclog"
)

const usage = `
                    __        _                     __
  _________  ____  / /_____ _(_)___  ___  _________/ /
 / ___/ __ \/ __ \/ __/ __ ` + "`" + `/ / __ \/ _ \/ ___/ __  /
/ /__/ /_/ / / / / /_/ /_/ / / / / /  __/ /  / /_/ /
\___/\____/_/ /_/\__/\__,_/_/_/ /_/\___/_/   \__,_/

high performance container runtime
`

func init() {
	// Discard grpc logs so that they don't mess with our stdio
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))

	cli.VersionPrinter = func(c *cli.Context) {
		fmt.Println(c.App.Name, version.Package, c.App.Version, version.Revision)
	}
}

// App returns a *cli.App instance.
func App() *cli.App {
	app := cli.NewApp()
	app.Name = "containerd"       // 实例化一个应用程序，名字叫做containerd
	app.Version = version.Version // 设置版本
	app.Usage = usage             // 表明containerd是一个高性能容器运行时？ TODO containerd的高性能表现在哪里？
	app.Description = `
containerd is a high performance container runtime whose daemon can be started
by using this command. If none of the *config*, *publish*, *oci-hook*, or *help* commands
are specified, the default action of the **containerd** command is to start the
containerd daemon in the foreground.


A default configuration is used if no TOML configuration is specified or located
at the default file location. The *containerd config* command can be used to
generate the default configuration for containerd. The output of that command
can be used and modified as necessary as a custom configuration.`
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "config,c", // containerd的配置文件，譬如代理配置，root目录配置，state目录配置，日志，插件等等，默认路径为/run/containerd/config.toml
			Usage: "Path to the configuration file",
			Value: filepath.Join(defaults.DefaultConfigDir, "config.toml"),
		},
		cli.StringFlag{
			Name:  "log-level,l", // TODO 默认是啥？
			Usage: "Set the logging level [trace, debug, info, warn, error, fatal, panic]",
		},
		cli.StringFlag{
			Name:  "address,a", // 这里应配置Container启动的GRPC服务的地址 TODO 默认是啥？
			Usage: "Address for containerd's GRPC server",
		},

		/*
			root@containerd:~# tree /var/lib/containerd/ -L 3
			/var/lib/containerd/
			├── io.containerd.content.v1.content
			│ ├── blobs
			│ │ └── sha256
			│ └── ingest
			├── io.containerd.metadata.v1.bolt
			│ └── meta.db
			├── io.containerd.runtime.v1.linux
			├── io.containerd.runtime.v2.task
			├── io.containerd.snapshotter.v1.btrfs
			├── io.containerd.snapshotter.v1.native
			│ └── snapshots
			├── io.containerd.snapshotter.v1.overlayfs
			│ ├── metadata.db
			│ └── snapshots
			│     ├── 1
			│     ├── 10
			│     ├── 11
			│     ├── 12
			│     ├── 13
			│     ├── 14
			│     ├── 15
			│     ├── 16
			│     ├── 17
			│     ├── 18
			│     ├── 2
			│     ├── 3
			│     ├── 4
			│     ├── 5
			│     ├── 6
			│     ├── 7
			│     ├── 8
			│     └── 9
			└── tmpmounts
		*/
		cli.StringFlag{
			Name:  "root", // Root地址， 默认值为/var/lib/containerd，保存的是containerd下载的镜像、运行中的容器
			Usage: "containerd root directory",
		},

		/*
		   root@containerd:~# tree /run/containerd/
		   /run/containerd/
		   ├── containerd.sock
		   ├── containerd.sock.ttrpc
		   ├── io.containerd.runtime.v1.linux
		   └── io.containerd.runtime.v2.task
		*/
		cli.StringFlag{
			Name:  "state", // 默认值为/run/containerd，保存的是containerd运行过程中创建的socket文件
			Usage: "containerd state directory",
		},
	}
	app.Flags = append(app.Flags, serviceFlags()...)
	// 可以看到，containerd非常简单，只有三个子命令。这是因为containerd提供的是以服务的方式启动的，需要通过客户端来和服务端通信。
	app.Commands = []cli.Command{
		configCommand,  // 用于生成containerd默认的配置以及查看containerd运行时的配置
		publishCommand, // TODO 暂时不知道这玩意干嘛的
		ociHook,        // TODO 似乎是为OCI容器注入Hook，我猜测这里的hook应该是针对于所有的容器，或者是这玩意可以配置选择器
	}
	app.Action = func(context *cli.Context) error {
		var (
			start       = time.Now()
			signals     = make(chan os.Signal, 2048)   // 最多同时接收2048个信号
			serverC     = make(chan *server.Server, 1) // TODO 我猜测这里的意思是最多只允许同时启动一个Server
			ctx, cancel = gocontext.WithCancel(gocontext.Background())
			config      = defaultConfig() // 生成默认的配置
		)

		defer cancel()

		// Only try to load the config if it either exists, or the user explicitly
		// told us to load this6 path.
		// 默认值就是/etc/containerd/config.toml
		configPath := context.GlobalString("config") // 获取配置文件路径
		_, err := os.Stat(configPath)
		// 如果不是没有找到/etc/containerd/config.toml配置文件的错误或者是设置了config参数
		if !os.IsNotExist(err) || context.GlobalIsSet("config") {
			// 加载containerd的配置，然后反序列化到config对象当中
			if err := srvconfig.LoadConfig(configPath, config); err != nil {
				return err
			}
		}

		// Apply flags to the config
		// 很简单，用户如果设置了root, state, address这三个参数，那么以用户在命令行中设置的参数为准，修改配置config实例对象
		if err := applyFlags(context, config); err != nil {
			return err
		}

		// 必须要监听某个地址，这里默认是/run/containerd/containerd.sock TODO 这种方式是通过UDS通信，难道containerd不可以通过端口通信？
		if config.GRPC.Address == "" {
			return fmt.Errorf("grpc address cannot be empty: %w", errdefs.ErrInvalidArgument)
		}
		// 所谓的TTRPC，其实就是GRPC over TLS，也就是安装的GRPC
		if config.TTRPC.Address == "" {
			// If TTRPC was not explicitly configured, use defaults based on GRPC.
			config.TTRPC.Address = config.GRPC.Address + ".ttrpc"
			config.TTRPC.UID = config.GRPC.UID
			config.TTRPC.GID = config.GRPC.GID
		}

		// Make sure top-level directories are created early.
		// 校验root、state目录必须存在，而且必须配置合适的权限
		if err := server.CreateTopLevelDirectories(config); err != nil {
			return err
		}

		// Stop if we are registering or unregistering against Windows SCM.
		// 仅和Windows有关
		stop, err := registerUnregisterService(config.Root)
		if err != nil {
			logrus.Fatal(err)
		}
		if stop {
			return nil
		}

		// 处理SIGTERM, SIGINT, SIGUSR1, SIGPIPE信号
		done := handleSignals(ctx, signals, serverC, cancel) // 处理退出信号
		// start the signal handler as soon as we can to make sure that
		// we don't miss any signals during boot
		// 当SIGTERM, SIGINT, SIGUSER, SIGPIPE信号发生时，会把这些信号写入到signals通道当中
		signal.Notify(signals, handledSignals...)

		// cleanup temp mounts 清理/var/lib/containerd/tmpmounts目录
		if err := mount.SetTempMountLocation(filepath.Join(config.Root, "tmpmounts")); err != nil {
			// 通过%w参数包裹错误，类似errors.wrap(err, "creating temp mount location")
			return fmt.Errorf("creating temp mount location: %w", err)
		}
		// unmount all temp mounts on boot for the server
		// TODO 这里应该是再清空tmpmount目录，这个目录有啥用？
		warnings, err := mount.CleanupTempMounts(0)
		if err != nil {
			log.G(ctx).WithError(err).Error("unmounting temp mounts")
		}
		for _, w := range warnings {
			log.G(ctx).WithError(w).Warn("cleanup temp mount")
		}

		// 下面就开始启动容器啦
		log.G(ctx).WithFields(log.Fields{
			"version":  version.Version,
			"revision": version.Revision,
		}).Info("starting containerd")

		type srvResp struct {
			s   *server.Server
			err error
		}

		// run server initialization in a goroutine so we don't end up blocking important things like SIGTERM handling
		// while the server is initializing.
		// As an example, opening the bolt database blocks forever if a containerd instance
		// is already running, which must then be forcibly terminated (SIGKILL) to recover.
		chsrv := make(chan srvResp)
		go func() {
			defer close(chsrv)

			// TODO 这里干了啥？
			// 1、这里主要要是为了实例化一个containerd实例
			server, err := server.New(ctx, config)
			if err != nil {
				select {
				case chsrv <- srvResp{err: err}:
				case <-ctx.Done():
				}
				return
			}

			// Launch as a Windows Service if necessary 这里主要是在适配windows，直接忽略
			if err := launchService(server, done); err != nil {
				logrus.Fatal(err)
			}
			select {
			case <-ctx.Done():
				server.Stop() // 一般是不会停止的
			case chsrv <- srvResp{s: server}: // 所以一般是到了这里，把这个containerd实例放入到了channel当中
			}
		}()

		var server *server.Server
		select { // 等待Containerd Server初始化完成
		case <-ctx.Done():
			return ctx.Err()
		case r := <-chsrv:
			if r.err != nil {
				return r.err
			}
			server = r.s
		}

		// We don't send the server down serverC directly in the goroutine above because we need it lower down.
		select { // TODO 这里为啥这么写，没看懂上面的注释
		case <-ctx.Done(): // TODO 上下文什么时候会被关闭
			return ctx.Err()
		case serverC <- server:
		}

		// 开启containerd的debug功能，开启后可以通过/debug/vars, /debug/pprof这样的URL查看containerd部分数据
		if config.Debug.Address != "" {
			var l net.Listener
			if isLocalAddress(config.Debug.Address) {
				if l, err = sys.GetLocalListener(config.Debug.Address, config.Debug.UID, config.Debug.GID); err != nil {
					return fmt.Errorf("failed to get listener for debug endpoint: %w", err)
				}
			} else {
				if l, err = net.Listen("tcp", config.Debug.Address); err != nil {
					return fmt.Errorf("failed to get listener for debug endpoint: %w", err)
				}
			}
			serve(ctx, l, server.ServeDebug)
		}
		// containerd的指标
		if config.Metrics.Address != "" {
			l, err := net.Listen("tcp", config.Metrics.Address)
			if err != nil {
				return fmt.Errorf("failed to get listener for metrics endpoint: %w", err)
			}
			serve(ctx, l, server.ServeMetrics)
		}
		// setup the ttrpc endpoint 创建containerd.sock.ttrpc文件
		tl, err := sys.GetLocalListener(config.TTRPC.Address, config.TTRPC.UID, config.TTRPC.GID)
		if err != nil {
			return fmt.Errorf("failed to get listener for main ttrpc endpoint: %w", err)
		}
		serve(ctx, tl, server.ServeTTRPC)

		// 一般我们是通过TCPAddress地址，通过网络来访问containerd提供的服务
		if config.GRPC.TCPAddress != "" {
			l, err := net.Listen("tcp", config.GRPC.TCPAddress)
			if err != nil {
				return fmt.Errorf("failed to get listener for TCP grpc endpoint: %w", err)
			}
			serve(ctx, l, server.ServeTCP)
		}
		// setup the main grpc endpoint 创建container.sock文件
		l, err := sys.GetLocalListener(config.GRPC.Address, config.GRPC.UID, config.GRPC.GID)
		if err != nil {
			return fmt.Errorf("failed to get listener for main endpoint: %w", err)
		}
		serve(ctx, l, server.ServeGRPC)

		readyC := make(chan struct{})
		go func() {
			server.Wait()
			close(readyC)
		}()

		select {
		case <-readyC:
			if err := notifyReady(ctx); err != nil {
				log.G(ctx).WithError(err).Warn("notify ready failed")
			}
			// containerd成功启动
			log.G(ctx).Infof("containerd successfully booted in %fs", time.Since(start).Seconds())
			<-done
		case <-done:
		}
		return nil
	}
	return app
}

func serve(ctx gocontext.Context, l net.Listener, serveFunc func(net.Listener) error) {
	path := l.Addr().String()
	log.G(ctx).WithField("address", path).Info("serving...")
	go func() {
		defer l.Close()
		if err := serveFunc(l); err != nil {
			log.G(ctx).WithError(err).WithField("address", path).Fatal("serve failure")
		}
	}()
}

// 很简单，用户如果设置了root, state, address这三个参数，那么以用户在命令行中设置的参数为准，修改配置config实例对象
func applyFlags(context *cli.Context, config *srvconfig.Config) error {
	// the order for config vs flag values is that flags will always override
	// the config values if they are set
	if err := setLogLevel(context, config); err != nil {
		return err
	}
	if err := setLogFormat(config); err != nil {
		return err
	}

	for _, v := range []struct {
		name string
		d    *string
	}{
		{
			name: "root",
			d:    &config.Root, // 这里取了指针，其目的是就是为了写覆盖
		},
		{
			name: "state",
			d:    &config.State,
		},
		{
			name: "address",
			d:    &config.GRPC.Address,
		},
	} {
		if s := context.GlobalString(v.name); s != "" {
			*v.d = s // 如果用户设置了参数，那么覆盖配置文件中的参数
			if v.name == "root" || v.name == "state" {
				absPath, err := filepath.Abs(s)
				if err != nil {
					return err
				}
				*v.d = absPath
			}
		}
	}

	// 空的，啥也不是
	applyPlatformFlags(context)

	return nil
}

func setLogLevel(context *cli.Context, config *srvconfig.Config) error {
	l := context.GlobalString("log-level")
	if l == "" {
		l = config.Debug.Level
	}
	if l != "" {
		lvl, err := logrus.ParseLevel(l)
		if err != nil {
			return err
		}
		logrus.SetLevel(lvl)
	}
	return nil
}

func setLogFormat(config *srvconfig.Config) error {
	f := config.Debug.Format
	if f == "" {
		f = log.TextFormat
	}

	switch f {
	case log.TextFormat:
		logrus.SetFormatter(&logrus.TextFormatter{
			TimestampFormat: log.RFC3339NanoFixed,
			FullTimestamp:   true,
		})
	case log.JSONFormat:
		logrus.SetFormatter(&logrus.JSONFormatter{
			TimestampFormat: log.RFC3339NanoFixed,
		})
	default:
		return fmt.Errorf("unknown log format: %s", f)
	}

	return nil
}

func dumpStacks(writeToFile bool) {
	var (
		buf       []byte
		stackSize int
	)
	bufferLen := 16384
	for stackSize == len(buf) {
		buf = make([]byte, bufferLen)
		stackSize = runtime.Stack(buf, true)
		bufferLen *= 2
	}
	buf = buf[:stackSize]
	logrus.Infof("=== BEGIN goroutine stack dump ===\n%s\n=== END goroutine stack dump ===", buf)

	if writeToFile {
		// Also write to file to aid gathering diagnostics
		name := filepath.Join(os.TempDir(), fmt.Sprintf("containerd.%d.stacks.log", os.Getpid()))
		f, err := os.Create(name)
		if err != nil {
			return
		}
		defer f.Close()
		f.WriteString(string(buf))
		logrus.Infof("goroutine stack dump written to %s", name)
	}
}
