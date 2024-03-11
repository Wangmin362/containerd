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
	"io"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/defaults"
	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/pkg/timeout"
	"github.com/containerd/containerd/services/server"
	srvconfig "github.com/containerd/containerd/services/server/config"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pelletier/go-toml"
	"github.com/urfave/cli"
)

// Config is a wrapper of server config for printing out.
type Config struct {
	*srvconfig.Config
	// Plugins overrides `Plugins map[string]toml.Tree` in server config.
	Plugins map[string]interface{} `toml:"plugins"`
}

// WriteTo marshals the config to the provided writer
func (c *Config) WriteTo(w io.Writer) (int64, error) {
	return 0, toml.NewEncoder(w).Encode(c)
}

func outputConfig(cfg *srvconfig.Config) error {
	config := &Config{
		Config: cfg,
	}

	plugins, err := server.LoadPlugins(gocontext.Background(), config.Config)
	if err != nil {
		return err
	}
	if len(plugins) != 0 {
		config.Plugins = make(map[string]interface{})
		for _, p := range plugins {
			if p.Config == nil {
				continue
			}

			pc, err := config.Decode(p)
			if err != nil {
				return err
			}

			config.Plugins[p.URI()] = pc
		}
	}

	if config.Timeouts == nil {
		config.Timeouts = make(map[string]string)
	}
	timeouts := timeout.All()
	for k, v := range timeouts {
		if config.Timeouts[k] == "" {
			config.Timeouts[k] = v.String()
		}
	}

	// for the time being, keep the defaultConfig's version set at 1 so that
	// when a config without a version is loaded from disk and has no version
	// set, we assume it's a v1 config.  But when generating new configs via
	// this command, generate the v2 config
	config.Config.Version = 2

	// remove overridden Plugins type to avoid duplication in output
	config.Config.Plugins = nil

	_, err = config.WriteTo(os.Stdout)
	return err
}

// 构造默认的配置
func defaultConfig() *srvconfig.Config {
	return platformAgnosticDefaultConfig()
}

var configCommand = cli.Command{
	Name:  "config",
	Usage: "Information on the containerd config",
	Subcommands: []cli.Command{
		{
			Name:  "default",
			Usage: "See the output of the default config",
			Action: func(context *cli.Context) error {
				return outputConfig(defaultConfig())
			},
		},
		{
			Name:  "dump",
			Usage: "See the output of the final main config with imported in subconfig files",
			Action: func(context *cli.Context) error {
				config := defaultConfig()
				if err := srvconfig.LoadConfig(context.GlobalString("config"), config); err != nil && !os.IsNotExist(err) {
					return err
				}

				return outputConfig(config)
			},
		},
	},
}

func platformAgnosticDefaultConfig() *srvconfig.Config {
	return &srvconfig.Config{
		// see: https://github.com/containerd/containerd/blob/5c6ea7fdc1247939edaddb1eba62a94527418687/RELEASES.md#daemon-configuration
		// this version MUST remain set to 1 until either there exists a means to
		// override / configure the default at the containerd cli .. or when
		// version 1 is no longer supported
		Version: 1,                        // 默认设置为1，实际上目前都是使用2  TODO 1和2有啥区别？ 如何从1升级到2，可以直接升级么？
		Root:    defaults.DefaultRootDir,  // 默认值为/var/lib/containerd，用于保存containerd下载的镜像、运行的容器、任务等等
		State:   defaults.DefaultStateDir, // 默认值为/run/containerd，用于保存containerd运行过程中创建的socket
		GRPC: srvconfig.GRPCConfig{ // 设置默认的GRPC配置
			Address:        defaults.DefaultAddress,        // 默认值为/run/containerd/containerd.sock
			MaxRecvMsgSize: defaults.DefaultMaxRecvMsgSize, // 设置GRPC接口最大可以接收16MB大小的数据
			MaxSendMsgSize: defaults.DefaultMaxSendMsgSize, // 设置GRPC接口最大可以发送16MB大小的数据
		},
		DisabledPlugins:  []string{},         // 禁用的插件
		RequiredPlugins:  []string{},         // TODO 什么叫做Required插件？
		StreamProcessors: streamProcessors(), // TODO 什么叫做流处理器？
	}
}

func streamProcessors() map[string]srvconfig.StreamProcessor {
	const (
		ctdDecoder = "ctd-decoder"
		basename   = "io.containerd.ocicrypt.decoder.v1"
	)
	decryptionKeysPath := filepath.Join(defaults.DefaultConfigDir, "ocicrypt", "keys")
	ctdDecoderArgs := []string{
		"--decryption-keys-path", decryptionKeysPath,
	}
	ctdDecoderEnv := []string{
		"OCICRYPT_KEYPROVIDER_CONFIG=" + filepath.Join(defaults.DefaultConfigDir, "ocicrypt", "ocicrypt_keyprovider.conf"),
	}
	return map[string]srvconfig.StreamProcessor{
		basename + ".tar.gzip": {
			Accepts: []string{images.MediaTypeImageLayerGzipEncrypted},
			Returns: ocispec.MediaTypeImageLayerGzip,
			Path:    ctdDecoder,
			Args:    ctdDecoderArgs,
			Env:     ctdDecoderEnv,
		},
		basename + ".tar": {
			Accepts: []string{images.MediaTypeImageLayerEncrypted},
			Returns: ocispec.MediaTypeImageLayer,
			Path:    ctdDecoder,
			Args:    ctdDecoderArgs,
			Env:     ctdDecoderEnv,
		},
	}
}
