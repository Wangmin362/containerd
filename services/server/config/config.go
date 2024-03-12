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

package config

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/imdario/mergo"
	"github.com/pelletier/go-toml"
	"github.com/sirupsen/logrus"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/plugin"
)

// NOTE: Any new map fields added also need to be handled in mergeConfig.

// Config provides containerd configuration data for the server
// containerd的全局配置
type Config struct {
	// 指定使用container配置的哪个版本的语法，现在一般都是使用版本2语法
	Version int `toml:"version"`
	// Root is the path to a directory where containerd will store persistent data
	// 存放元数据的位置，譬如镜像，运行时数据等等，默认值为：/var/lib/containerd
	Root string `toml:"root"`
	// State is the path to a directory where containerd will store transient data
	// state配置用于设置containerd的socket文件的存放位置
	State string `toml:"state"`
	// TempDir is the path to a directory where to place containerd temporary files
	// TODO 临时目录是用来干嘛的？
	TempDir string `toml:"temp"`
	// PluginDir is the directory for dynamic plugins to be stored
	// TODO containerd的动态插件原理是啥？
	// 1、此参数默认是空的，通过containerd config default导出来的默认参数就是空的
	// 2、在程序中，如果发现这个参数是空的，那么默认使用<Root>/plugins，一般来说就是/var/lib/containerd/plugins
	PluginDir string `toml:"plugin_dir"`
	// GRPC configuration settings
	GRPC GRPCConfig `toml:"grpc"`
	// TTRPC configuration settings
	// 所谓的TTRPC实际上就是GRPC Over TLS，也就是加了密的GRPC
	TTRPC TTRPCConfig `toml:"ttrpc"`
	// Debug and profiling settings
	// 是否开启debug功能，开启后可以通过诸如/debug/vars, /debug/pprof类似的URL查看containerd的一些数据
	Debug Debug `toml:"debug"`
	// Metrics and monitoring settings
	// containerd的指标配置
	Metrics MetricsConfig `toml:"metrics"`
	// DisabledPlugins are IDs of plugins to disable. Disabled plugins won't be
	// initialized and started.
	// 希望被禁用的插件
	DisabledPlugins []string `toml:"disabled_plugins"`
	// RequiredPlugins are IDs of required plugins. Containerd exits if any
	// required plugin doesn't exist or fails to be initialized or started.
	// 1、所谓必须的插件，是针对于用户来说的。在使用containerd的时候，用户可以指定自己需要使用的插件，通过RequiredPlugins这个参数来声明。
	// containerd启动的时候会加载所有的插件，如果所有的插件都加载完成了，还是有没有加载到用户需要的插件，此时containerd就会任务初始化异常，
	// 然后退出containerd的启动
	// 2、TODO 对于用户来说，怎么知道当前containerd有那些插件？ 怎么知道每个插件的名字应该叫什么名字？
	RequiredPlugins []string `toml:"required_plugins"`
	// Plugins provides plugin specific configuration for the initialization of a plugin
	// 各个插件的配置  TODO 一个具体的插件都有哪些配置项？从哪里获取？
	Plugins map[string]toml.Tree `toml:"plugins"`
	// OOMScore adjust the containerd's oom score
	// TODO 如何理解这个参数？
	OOMScore int `toml:"oom_score"`
	// Cgroup specifies cgroup information for the containerd daemon process
	// TODO 这个参数有啥用？ 默认值是空的
	Cgroup CgroupConfig `toml:"cgroup"`
	// ProxyPlugins configures plugins which are communicated to over GRPC
	// TODO containerd的代理插件有啥用？
	ProxyPlugins map[string]ProxyPlugin `toml:"proxy_plugins"`
	// Timeouts specified as a duration
	// 超时参数设置，containerd默认设置的参数如下：
	/*
	  "io.containerd.timeout.bolt.open" = "0s"
	  "io.containerd.timeout.metrics.shimstats" = "2s"
	  "io.containerd.timeout.shim.cleanup" = "5s"
	  "io.containerd.timeout.shim.load" = "5s"
	  "io.containerd.timeout.shim.shutdown" = "3s"
	  "io.containerd.timeout.task.state" = "2s"
	*/
	Timeouts map[string]string `toml:"timeouts"`
	// Imports are additional file path list to config files that can overwrite main config file fields
	// 这个参数还是比较容易理解，通过这个配置可以把containerd参数分散写在多个地方，然后通过imports参数导入进来
	Imports []string `toml:"imports"`
	// StreamProcessors configuration
	// TODO 如何理解流处理器
	StreamProcessors map[string]StreamProcessor `toml:"stream_processors"`
}

// StreamProcessor provides configuration for diff content processors
type StreamProcessor struct {
	// Accepts specific media-types
	Accepts []string `toml:"accepts"`
	// Returns the media-type
	Returns string `toml:"returns"`
	// Path or name of the binary
	Path string `toml:"path"`
	// Args to the binary
	Args []string `toml:"args"`
	// Environment variables for the binary
	Env []string `toml:"env"`
}

// GetVersion returns the config file's version
func (c *Config) GetVersion() int {
	if c.Version == 0 {
		return 1
	}
	return c.Version
}

// ValidateV2 validates the config for a v2 file
func (c *Config) ValidateV2() error {
	version := c.GetVersion()
	if version < 2 {
		logrus.Warnf("containerd config version `%d` has been deprecated and will be removed in containerd v2.0, please switch to version `2`, "+
			"see https://github.com/containerd/containerd/blob/main/docs/PLUGINS.md#version-header", version)
		return nil
	}
	for _, p := range c.DisabledPlugins {
		if !strings.HasPrefix(p, "io.containerd.") || len(strings.SplitN(p, ".", 4)) < 4 {
			return fmt.Errorf("invalid disabled plugin URI %q expect io.containerd.x.vx", p)
		}
	}
	for _, p := range c.RequiredPlugins {
		if !strings.HasPrefix(p, "io.containerd.") || len(strings.SplitN(p, ".", 4)) < 4 {
			return fmt.Errorf("invalid required plugin URI %q expect io.containerd.x.vx", p)
		}
	}
	for p := range c.Plugins {
		if !strings.HasPrefix(p, "io.containerd.") || len(strings.SplitN(p, ".", 4)) < 4 {
			return fmt.Errorf("invalid plugin key URI %q expect io.containerd.x.vx", p)
		}
	}
	return nil
}

// GRPCConfig provides GRPC configuration for the socket
type GRPCConfig struct {
	Address        string `toml:"address"` // UDS地址，也称为本地地址，因为是通过UDS通信
	TCPAddress     string `toml:"tcp_address"`
	TCPTLSCA       string `toml:"tcp_tls_ca"`
	TCPTLSCert     string `toml:"tcp_tls_cert"`
	TCPTLSKey      string `toml:"tcp_tls_key"`
	UID            int    `toml:"uid"`
	GID            int    `toml:"gid"`
	MaxRecvMsgSize int    `toml:"max_recv_message_size"`
	MaxSendMsgSize int    `toml:"max_send_message_size"`
}

// TTRPCConfig provides TTRPC configuration for the socket
type TTRPCConfig struct {
	Address string `toml:"address"`
	UID     int    `toml:"uid"`
	GID     int    `toml:"gid"`
}

// Debug provides debug configuration
type Debug struct {
	Address string `toml:"address"`
	UID     int    `toml:"uid"`
	GID     int    `toml:"gid"`
	Level   string `toml:"level"`
	// Format represents the logging format. Supported values are 'text' and 'json'.
	Format string `toml:"format"`
}

// MetricsConfig provides metrics configuration
type MetricsConfig struct {
	Address       string `toml:"address"`
	GRPCHistogram bool   `toml:"grpc_histogram"`
}

// CgroupConfig provides cgroup configuration
type CgroupConfig struct {
	Path string `toml:"path"`
}

// ProxyPlugin provides a proxy plugin configuration
type ProxyPlugin struct {
	Type    string `toml:"type"`
	Address string `toml:"address"`
}

// Decode unmarshals a plugin specific configuration by plugin id
func (c *Config) Decode(p *plugin.Registration) (interface{}, error) {
	id := p.URI()
	if c.GetVersion() == 1 {
		id = p.ID
	}

	// 读取插件配置
	data, ok := c.Plugins[id]
	if !ok {
		return p.Config, nil
	}
	// 反序列化
	if err := data.Unmarshal(p.Config); err != nil {
		return nil, err
	}
	return p.Config, nil
}

// LoadConfig loads the containerd server config from the provided path
// 读取path路径指定的containerd配置文件，并把配置反序列化到out对象当中
func LoadConfig(path /* 默认值为/etc/containerd/config.toml */ string, out *Config) error {
	if out == nil {
		return fmt.Errorf("argument out must not be nil: %w", errdefs.ErrInvalidArgument)
	}

	var (
		loaded  = map[string]bool{}
		pending = []string{path}
	)

	for len(pending) > 0 {
		path, pending = pending[0], pending[1:]

		// Check if a file at the given path already loaded to prevent circular imports
		if _, ok := loaded[path]; ok {
			continue
		}

		config, err := loadConfigFile(path)
		if err != nil {
			return err
		}

		if err := mergeConfig(out, config); err != nil {
			return err
		}

		imports, err := resolveImports(path, config.Imports)
		if err != nil {
			return err
		}

		loaded[path] = true
		pending = append(pending, imports...)
	}

	// Fix up the list of config files loaded
	out.Imports = []string{}
	for path := range loaded {
		out.Imports = append(out.Imports, path)
	}

	err := out.ValidateV2()
	if err != nil {
		return fmt.Errorf("failed to load TOML from %s: %w", path, err)
	}
	return nil
}

// loadConfigFile decodes a TOML file at the given path
func loadConfigFile(path string) (*Config, error) {
	config := &Config{}

	file, err := toml.LoadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to load TOML: %s: %w", path, err)
	}

	if err := file.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal TOML: %w", err)
	}

	return config, nil
}

// resolveImports resolves import strings list to absolute paths list:
// - If path contains *, glob pattern matching applied
// - Non abs path is relative to parent config file directory
// - Abs paths returned as is
func resolveImports(parent string, imports []string) ([]string, error) {
	var out []string

	for _, path := range imports {
		if strings.Contains(path, "*") {
			matches, err := filepath.Glob(path)
			if err != nil {
				return nil, err
			}

			out = append(out, matches...)
		} else {
			path = filepath.Clean(path)
			if !filepath.IsAbs(path) {
				path = filepath.Join(filepath.Dir(parent), path)
			}

			out = append(out, path)
		}
	}

	return out, nil
}

// mergeConfig merges Config structs with the following rules:
// 'to'         'from'      'result'
// ""           "value"     "value"
// "value"      ""          "value"
// 1            0           1
// 0            1           1
// []{"1"}      []{"2"}     []{"1","2"}
// []{"1"}      []{}        []{"1"}
// Maps merged by keys, but values are replaced entirely.
func mergeConfig(to, from *Config) error {
	err := mergo.Merge(to, from, mergo.WithOverride, mergo.WithAppendSlice)
	if err != nil {
		return err
	}

	// Replace entire sections instead of merging map's values.
	for k, v := range from.Plugins {
		to.Plugins[k] = v
	}

	for k, v := range from.StreamProcessors {
		to.StreamProcessors[k] = v
	}

	for k, v := range from.ProxyPlugins {
		to.ProxyPlugins[k] = v
	}

	for k, v := range from.Timeouts {
		to.Timeouts[k] = v
	}

	return nil
}

// V1DisabledFilter matches based on ID
func V1DisabledFilter(list []string) plugin.DisableFilter {
	set := make(map[string]struct{}, len(list))
	for _, l := range list {
		set[l] = struct{}{}
	}
	return func(r *plugin.Registration) bool {
		_, ok := set[r.ID]
		return ok
	}
}

// V2DisabledFilter matches based on URI
func V2DisabledFilter(list []string) plugin.DisableFilter {
	// 把列表转换为一个map，把时间复杂到降低为O(1)
	set := make(map[string]struct{}, len(list))
	for _, l := range list {
		set[l] = struct{}{}
	}
	return func(r *plugin.Registration) bool {
		_, ok := set[r.URI()]
		return ok
	}
}
