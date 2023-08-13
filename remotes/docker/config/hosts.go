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

// Package config contains utilities for helping configure the Docker resolver
package config

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/pelletier/go-toml"
)

// UpdateClientFunc is a function that lets you to amend http Client behavior used by registry clients.
type UpdateClientFunc func(client *http.Client) error

/*
cat > /etc/containerd/certs.d/docker.io/hosts.toml << EOF
server = "https://docker.io"
[host."https://dockerproxy.com"]
  capabilities = ["pull", "resolve"]

[host."https://docker.m.daocloud.io"]
  capabilities = ["pull", "resolve"]

[host."https://reg-mirror.qiniu.com"]
  capabilities = ["pull", "resolve"]

[host."https://registry.docker-cn.com"]
  capabilities = ["pull", "resolve"]

[host."http://hub-mirror.c.163.com"]
  capabilities = ["pull", "resolve"]
EOF
*/
// hostConfig实际上可以理解为/etc/containerd/certs.d/<repository>配置
type hostConfig struct {
	scheme string // http还是https
	host   string // host域名
	path   string // URL

	// 镜像仓库支持的能力
	capabilities docker.HostCapabilities

	caCerts     []string
	clientPairs [][2]string
	// 是否跳过HTTPS证书校验
	skipVerify *bool

	// 额外添加的请求头
	header http.Header

	// TODO: Add credential configuration (domain alias, username)
}

// HostOptions is used to configure registry hosts
type HostOptions struct {
	HostDir       func(string) (string, error)
	Credentials   func(host string) (string, string, error)
	DefaultTLS    *tls.Config
	DefaultScheme string
	// UpdateClient will be called after creating http.Client object, so clients can provide extra configuration
	UpdateClient   UpdateClientFunc
	AuthorizerOpts []docker.AuthorizerOpt
}

// ConfigureHosts creates a registry hosts function from the provided
// host creation options. The host directory can read hosts.toml or
// certificate files laid out in the Docker specific layout.
// If a `HostDir` function is not required, defaults are used.
// ConfigureHosts函数的返回值为一个函数，该函数的作用是根据传入的host域名，在/etc/containerd/certs.d目录中找到这个host域名的镜像仓库
// 配置，然后把这个配置解析出来，获取到拉取Host域名的镜像所支持的镜像仓库的客户端
func ConfigureHosts(ctx context.Context, options HostOptions) docker.RegistryHosts {
	return func(host string) ([]docker.RegistryHost, error) {
		// hostConfig实际上可以理解为/etc/containerd/certs.d/<repository>配置
		var hosts []hostConfig
		if options.HostDir != nil {
			// HostDir函数的作用就是在/etc/containerd/certs.d目录中找到参数host域名的仓库配置
			dir, err := options.HostDir(host)
			if err != nil && !errdefs.IsNotFound(err) {
				return nil, err
			}
			if dir != "" {
				log.G(ctx).WithField("dir", dir).Debug("loading host directory")
				// 1、加载配置，dir目录为：/etc/containerd/certs.d/<host>
				// 2、解析/etc/containerd/certs.d/<host>/hosts.toml配置
				hosts, err = loadHostDir(ctx, dir)
				if err != nil {
					return nil, err
				}
			}
		}

		// If hosts was not set, add a default host
		// NOTE: Check nil here and not empty, the host may be
		// intentionally configured to not have any endpoints
		if hosts == nil {
			hosts = make([]hostConfig, 1)
		}
		// 什么时候会符合这个条件？，可以看到上面这个判断，如果没有配置/etc/containerd/certs.d目录，那么解析出来的hosts文件肯定为空
		// 所以就会进入到下面这个逻辑
		if len(hosts) > 0 && hosts[len(hosts)-1].host == "" {
			// 如果请求的是docker官方镜像仓库，那么默认使用：https://registry-1.docker.io进行请求
			if host == "docker.io" {
				hosts[len(hosts)-1].scheme = "https"
				hosts[len(hosts)-1].host = "registry-1.docker.io"
			} else if docker.IsLocalhost(host) {
				hosts[len(hosts)-1].host = host
				if options.DefaultScheme == "" || options.DefaultScheme == "http" {
					hosts[len(hosts)-1].scheme = "http"

					// Skipping TLS verification for localhost
					var skipVerify = true
					hosts[len(hosts)-1].skipVerify = &skipVerify
				} else {
					hosts[len(hosts)-1].scheme = options.DefaultScheme
				}
			} else {
				hosts[len(hosts)-1].host = host
				// 如果设置了plain-http=true，那么这里就会使用http的方式请求镜像仓库
				if options.DefaultScheme != "" {
					hosts[len(hosts)-1].scheme = options.DefaultScheme
				} else {
					hosts[len(hosts)-1].scheme = "https"
				}
			}
			// TODO 这里的V2，指的是镜像的第二个标准？
			hosts[len(hosts)-1].path = "/v2"
			hosts[len(hosts)-1].capabilities = docker.HostCapabilityPull | docker.HostCapabilityResolve | docker.HostCapabilityPush
		}

		var defaultTLSConfig *tls.Config
		if options.DefaultTLS != nil {
			defaultTLSConfig = options.DefaultTLS
		} else {
			defaultTLSConfig = &tls.Config{}
		}

		defaultTransport := &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:       30 * time.Second,
				KeepAlive:     30 * time.Second,
				FallbackDelay: 300 * time.Millisecond,
			}).DialContext,
			MaxIdleConns:          10,
			IdleConnTimeout:       30 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			TLSClientConfig:       defaultTLSConfig,
			ExpectContinueTimeout: 5 * time.Second,
		}

		client := &http.Client{
			Transport: defaultTransport,
		}
		if options.UpdateClient != nil {
			if err := options.UpdateClient(client); err != nil {
				return nil, err
			}
		}

		authOpts := []docker.AuthorizerOpt{docker.WithAuthClient(client)}
		if options.Credentials != nil {
			authOpts = append(authOpts, docker.WithAuthCreds(options.Credentials))
		}
		authOpts = append(authOpts, options.AuthorizerOpts...)
		// TODO 这里的认证器是如何工作的？ 可以参考：https://docs.docker.com/registry/spec/auth/
		authorizer := docker.NewDockerAuthorizer(authOpts...)

		// 把hostConfig配置加工成为RegistryHost，也就是不同的镜像仓库客户端配置
		rhosts := make([]docker.RegistryHost, len(hosts))
		for i, host := range hosts {

			rhosts[i].Scheme = host.scheme
			rhosts[i].Host = host.host
			rhosts[i].Path = host.path
			rhosts[i].Capabilities = host.capabilities
			rhosts[i].Header = host.header

			if host.caCerts != nil || host.clientPairs != nil || host.skipVerify != nil {
				tr := defaultTransport.Clone()
				tlsConfig := tr.TLSClientConfig
				if host.skipVerify != nil {
					tlsConfig.InsecureSkipVerify = *host.skipVerify
				}
				if host.caCerts != nil {
					if tlsConfig.RootCAs == nil {
						rootPool, err := rootSystemPool()
						if err != nil {
							return nil, fmt.Errorf("unable to initialize cert pool: %w", err)
						}
						tlsConfig.RootCAs = rootPool
					}
					for _, f := range host.caCerts {
						data, err := os.ReadFile(f)
						if err != nil {
							return nil, fmt.Errorf("unable to read CA cert %q: %w", f, err)
						}
						if !tlsConfig.RootCAs.AppendCertsFromPEM(data) {
							return nil, fmt.Errorf("unable to load CA cert %q", f)
						}
					}
				}

				if host.clientPairs != nil {
					for _, pair := range host.clientPairs {
						certPEMBlock, err := os.ReadFile(pair[0])
						if err != nil {
							return nil, fmt.Errorf("unable to read CERT file %q: %w", pair[0], err)
						}
						var keyPEMBlock []byte
						if pair[1] != "" {
							keyPEMBlock, err = os.ReadFile(pair[1])
							if err != nil {
								return nil, fmt.Errorf("unable to read CERT file %q: %w", pair[1], err)
							}
						} else {
							// Load key block from same PEM file
							keyPEMBlock = certPEMBlock
						}
						cert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
						if err != nil {
							return nil, fmt.Errorf("failed to load X509 key pair: %w", err)
						}

						tlsConfig.Certificates = append(tlsConfig.Certificates, cert)
					}
				}

				c := *client
				c.Transport = tr
				if options.UpdateClient != nil {
					if err := options.UpdateClient(&c); err != nil {
						return nil, err
					}
				}

				rhosts[i].Client = &c
				rhosts[i].Authorizer = docker.NewDockerAuthorizer(append(authOpts, docker.WithAuthClient(&c))...)
			} else {
				rhosts[i].Client = client
				rhosts[i].Authorizer = authorizer
			}
		}

		return rhosts, nil
	}

}

// HostDirFromRoot returns a function which finds a host directory
// based at the given root.
// HostDirFromRoot的返回值是一个函数，参数为一个host域名，返回值为/etc/containerd/certs.d/<repository>目录
// 这个函数的作用就是在/etc/containerd/certs.d目录中找到参数host域名的仓库配置
func HostDirFromRoot(root string) func(string) (string, error) {
	// root参数一般设置为：/etc/containerd/certs.d目录
	return func(host string) (string, error) {
		// 1、用于获取/etc/containerd/certs.d目录中配置的仓库
		// 2、从这里可以看出，仓库的使用是有顺序的，先使用不带端口的，然后使用带端口的，最后使用/etc/containerd/certs.d/_default配置
		for _, p := range hostPaths(root, host) {
			if _, err := os.Stat(p); err == nil {
				return p, nil
			} else if !os.IsNotExist(err) {
				return "", err
			}
		}
		return "", errdefs.ErrNotFound
	}
}

// hostDirectory converts ":port" to "_port_" in directory names
// 如果当前仓库配置了端口，那么需要把端口去掉，才能取出host
func hostDirectory(host string) string {
	idx := strings.LastIndex(host, ":")
	if idx > 0 {
		return host[:idx] + "_" + host[idx+1:] + "_"
	}
	return host
}

// 1、hostDir目录为：/etc/containerd/certs.d/<host>
func loadHostDir(ctx context.Context, hostsDir string) ([]hostConfig, error) {
	// 读取/etc/containerd/certs.d/<host>/hosts.toml配置
	b, err := os.ReadFile(filepath.Join(hostsDir, "hosts.toml"))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	if len(b) == 0 {
		// If hosts.toml does not exist, fallback to checking for
		// certificate files based on Docker's certificate file
		// pattern (".crt", ".cert", ".key" files)
		// 如果不存在/etc/containerd/certs.d/<host>/hosts.toml文件，那么就从证书当中获取
		return loadCertFiles(ctx, hostsDir)
	}

	// 解析/etc/containerd/certs.d/<host>/hosts.toml配置
	hosts, err := parseHostsFile(hostsDir, b)
	if err != nil {
		log.G(ctx).WithError(err).Error("failed to decode hosts.toml")
		// Fallback to checking certificate files
		return loadCertFiles(ctx, hostsDir)
	}

	return hosts, nil
}

// /etc/containerd/certs.d/<host>/hosts.toml文件当中尽可以配置这些内容
type hostFileConfig struct {
	// Capabilities determine what operations a host is
	// capable of performing. Allowed values
	//  - pull
	//  - resolve
	//  - push
	// 镜像仓库，仅仅支持pull，resolve,push能力
	Capabilities []string `toml:"capabilities"`

	// CACert are the public key certificates for TLS
	// Accepted types
	// - string - Single file with certificate(s)
	// - []string - Multiple files with certificates
	// 当前仓库的证书
	CACert interface{} `toml:"ca"`

	// Client keypair(s) for TLS with client authentication
	// Accepted types
	// - string - Single file with public and private keys
	// - []string - Multiple files with public and private keys
	// - [][2]string - Multiple keypairs with public and private keys in separate files
	Client interface{} `toml:"client"`

	// SkipVerify skips verification of the server's certificate chain
	// and host name. This should only be used for testing or in
	// combination with other methods of verifying connections.
	// 是否跳过校验镜像仓库的证书
	SkipVerify *bool `toml:"skip_verify"`

	// Header are additional header files to send to the server
	// 向镜像仓库发起请求时，可以额外添加的请求头
	Header map[string]interface{} `toml:"header"`

	// OverridePath indicates the API root endpoint is defined in the URL
	// path rather than by the API specification.
	// This may be used with non-compliant OCI registries to override the
	// API root endpoint.
	// 是否使用配置中的URL覆盖
	OverridePath bool `toml:"override_path"`

	// TODO: Credentials: helper? name? username? alternate domain? token?
}

/*
cat > /etc/containerd/certs.d/docker.io/hosts.toml << EOF
server = "https://docker.io"
[host."https://dockerproxy.com"]
  capabilities = ["pull", "resolve"]

[host."https://docker.m.daocloud.io"]
  capabilities = ["pull", "resolve"]

[host."https://reg-mirror.qiniu.com"]
  capabilities = ["pull", "resolve"]

[host."https://registry.docker-cn.com"]
  capabilities = ["pull", "resolve"]

[host."http://hub-mirror.c.163.com"]
  capabilities = ["pull", "resolve"]
EOF
*/
// 一个/etc/containerd/certs.d/<host>/hosts.toml文件当中，可能会配置多个镜像仓库，所以这里的返回值为数组
func parseHostsFile(baseDir string, b []byte) ([]hostConfig, error) {
	tree, err := toml.LoadBytes(b)
	if err != nil {
		return nil, fmt.Errorf("failed to parse TOML: %w", err)
	}

	// HACK: we want to keep toml parsing structures private in this package, however go-toml ignores private embedded types.
	// so we remap it to a public type within the func body, so technically it's public, but not possible to import elsewhere.
	//nolint:unused
	type HostFileConfig = hostFileConfig

	c := struct {
		HostFileConfig
		// Server specifies the default server. When `host` is
		// also specified, those hosts are tried first.
		Server string `toml:"server"`
		// HostConfigs store the per-host configuration
		HostConfigs map[string]hostFileConfig `toml:"host"`
	}{}

	// 获取host配置，以定义的顺序进行排序，所以如果配置镜像加速，那么肯定是需要把速度最快的镜像仓库写在前面
	orderedHosts, err := getSortedHosts(tree)
	if err != nil {
		return nil, err
	}

	var (
		hosts []hostConfig
	)

	if err := tree.Unmarshal(&c); err != nil {
		return nil, err
	}

	// Parse hosts array
	for _, host := range orderedHosts {
		config := c.HostConfigs[host]

		parsed, err := parseHostConfig(host, baseDir, config)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, parsed)
	}

	// Parse root host config and append it as the last element
	parsed, err := parseHostConfig(c.Server, baseDir, c.HostFileConfig)
	if err != nil {
		return nil, err
	}
	hosts = append(hosts, parsed)

	return hosts, nil
}

func parseHostConfig(server string, baseDir string, config hostFileConfig) (hostConfig, error) {
	var (
		result = hostConfig{}
		err    error
	)

	if server != "" {
		if !strings.HasPrefix(server, "http") {
			server = "https://" + server
		}
		u, err := url.Parse(server)
		if err != nil {
			return hostConfig{}, fmt.Errorf("unable to parse server %v: %w", server, err)
		}
		result.scheme = u.Scheme
		result.host = u.Host
		if len(u.Path) > 0 {
			u.Path = path.Clean(u.Path)
			if !strings.HasSuffix(u.Path, "/v2") && !config.OverridePath {
				u.Path = u.Path + "/v2"
			}
		} else if !config.OverridePath {
			u.Path = "/v2"
		}
		result.path = u.Path
	}

	result.skipVerify = config.SkipVerify

	if len(config.Capabilities) > 0 {
		for _, c := range config.Capabilities {
			switch strings.ToLower(c) {
			case "pull":
				result.capabilities |= docker.HostCapabilityPull
			case "resolve":
				result.capabilities |= docker.HostCapabilityResolve
			case "push":
				result.capabilities |= docker.HostCapabilityPush
			default:
				return hostConfig{}, fmt.Errorf("unknown capability %v", c)
			}
		}
	} else {
		result.capabilities = docker.HostCapabilityPull | docker.HostCapabilityResolve | docker.HostCapabilityPush
	}

	if config.CACert != nil {
		switch cert := config.CACert.(type) {
		case string:
			result.caCerts = []string{makeAbsPath(cert, baseDir)}
		case []interface{}:
			result.caCerts, err = makeStringSlice(cert, func(p string) string {
				return makeAbsPath(p, baseDir)
			})
			if err != nil {
				return hostConfig{}, err
			}
		default:
			return hostConfig{}, fmt.Errorf("invalid type %v for \"ca\"", cert)
		}
	}

	if config.Client != nil {
		switch client := config.Client.(type) {
		case string:
			result.clientPairs = [][2]string{{makeAbsPath(client, baseDir), ""}}
		case []interface{}:
			// []string or [][2]string
			for _, pairs := range client {
				switch p := pairs.(type) {
				case string:
					result.clientPairs = append(result.clientPairs, [2]string{makeAbsPath(p, baseDir), ""})
				case []interface{}:
					slice, err := makeStringSlice(p, func(s string) string {
						return makeAbsPath(s, baseDir)
					})
					if err != nil {
						return hostConfig{}, err
					}
					if len(slice) != 2 {
						return hostConfig{}, fmt.Errorf("invalid pair %v for \"client\"", p)
					}

					var pair [2]string
					copy(pair[:], slice)
					result.clientPairs = append(result.clientPairs, pair)
				default:
					return hostConfig{}, fmt.Errorf("invalid type %T for \"client\"", p)
				}
			}
		default:
			return hostConfig{}, fmt.Errorf("invalid type %v for \"client\"", client)
		}
	}

	if config.Header != nil {
		header := http.Header{}
		for key, ty := range config.Header {
			switch value := ty.(type) {
			case string:
				header[key] = []string{value}
			case []interface{}:
				header[key], err = makeStringSlice(value, nil)
				if err != nil {
					return hostConfig{}, err
				}
			default:
				return hostConfig{}, fmt.Errorf("invalid type %v for header %q", ty, key)
			}
		}
		result.header = header
	}

	return result, nil
}

// getSortedHosts returns the list of hosts as they defined in the file.
func getSortedHosts(root *toml.Tree) ([]string, error) {
	iter, ok := root.Get("host").(*toml.Tree)
	if !ok {
		return nil, errors.New("invalid `host` tree")
	}

	list := append([]string{}, iter.Keys()...)

	// go-toml stores TOML sections in the map object, so no order guaranteed.
	// We retrieve line number for each key and sort the keys by position.
	sort.Slice(list, func(i, j int) bool {
		h1 := iter.GetPath([]string{list[i]}).(*toml.Tree)
		h2 := iter.GetPath([]string{list[j]}).(*toml.Tree)
		return h1.Position().Line < h2.Position().Line
	})

	return list, nil
}

// makeStringSlice is a helper func to convert from []interface{} to []string.
// Additionally an optional cb func may be passed to perform string mapping.
func makeStringSlice(slice []interface{}, cb func(string) string) ([]string, error) {
	out := make([]string, len(slice))
	for i, value := range slice {
		str, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("unable to cast %v to string", value)
		}

		if cb != nil {
			out[i] = cb(str)
		} else {
			out[i] = str
		}
	}
	return out, nil
}

func makeAbsPath(p string, base string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(base, p)
}

// loadCertsDir loads certs from certsDir like "/etc/docker/certs.d" .
// Compatible with Docker file layout
//   - files ending with ".crt" are treated as CA certificate files
//   - files ending with ".cert" are treated as client certificates, and
//     files with the same name but ending with ".key" are treated as the
//     corresponding private key.
//     NOTE: If a ".key" file is missing, this function will just return
//     the ".cert", which may contain the private key. If the ".cert" file
//     does not contain the private key, the caller should detect and error.
func loadCertFiles(ctx context.Context, certsDir string) ([]hostConfig, error) {
	fs, err := os.ReadDir(certsDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	hosts := make([]hostConfig, 1)
	for _, f := range fs {
		if f.IsDir() {
			continue
		}
		if strings.HasSuffix(f.Name(), ".crt") {
			hosts[0].caCerts = append(hosts[0].caCerts, filepath.Join(certsDir, f.Name()))
		}
		if strings.HasSuffix(f.Name(), ".cert") {
			var pair [2]string
			certFile := f.Name()
			pair[0] = filepath.Join(certsDir, certFile)
			// Check if key also exists
			keyFile := filepath.Join(certsDir, certFile[:len(certFile)-5]+".key")
			if _, err := os.Stat(keyFile); err == nil {
				pair[1] = keyFile
			} else if !os.IsNotExist(err) {
				return nil, err
			}
			hosts[0].clientPairs = append(hosts[0].clientPairs, pair)
		}
	}
	return hosts, nil
}
