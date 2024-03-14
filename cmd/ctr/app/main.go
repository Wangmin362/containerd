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

package app

import (
	"fmt"
	"io"

	"github.com/containerd/containerd/cmd/ctr/commands/containers"
	"github.com/containerd/containerd/cmd/ctr/commands/content"
	"github.com/containerd/containerd/cmd/ctr/commands/events"
	"github.com/containerd/containerd/cmd/ctr/commands/images"
	"github.com/containerd/containerd/cmd/ctr/commands/info"
	"github.com/containerd/containerd/cmd/ctr/commands/install"
	"github.com/containerd/containerd/cmd/ctr/commands/leases"
	namespacesCmd "github.com/containerd/containerd/cmd/ctr/commands/namespaces"
	ociCmd "github.com/containerd/containerd/cmd/ctr/commands/oci"
	"github.com/containerd/containerd/cmd/ctr/commands/plugins"
	"github.com/containerd/containerd/cmd/ctr/commands/pprof"
	"github.com/containerd/containerd/cmd/ctr/commands/run"
	"github.com/containerd/containerd/cmd/ctr/commands/sandboxes"
	"github.com/containerd/containerd/cmd/ctr/commands/snapshots"
	"github.com/containerd/containerd/cmd/ctr/commands/tasks"
	versionCmd "github.com/containerd/containerd/cmd/ctr/commands/version"
	"github.com/containerd/containerd/defaults"
	"github.com/containerd/containerd/namespaces"
	"github.com/containerd/containerd/version"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc/grpclog"
)

// TODO 目前看来，shim会注册到这里面
var extraCmds []cli.Command

func init() {
	// Discard grpc logs so that they don't mess with our stdio
	// TODO 初始化日志
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))

	cli.VersionPrinter = func(c *cli.Context) {
		// 打印版本的格式
		fmt.Println(c.App.Name, version.Package, c.App.Version)
	}
}

// New returns a *cli.App instance.
func New() *cli.App {
	app := cli.NewApp() // 实例化一个命令行工具
	app.Name = "ctr"
	app.Version = version.Version
	app.Description = `
ctr is an unsupported debug and administrative client for interacting
with the containerd daemon. Because it is unsupported, the commands,
options, and operations are not guaranteed to be backward compatible or
stable from release to release of the containerd project.`
	app.Usage = `
        __
  _____/ /______
 / ___/ __/ ___/
/ /__/ /_/ /
\___/\__/_/

containerd CLI
`
	app.EnableBashCompletion = true
	app.Flags = []cli.Flag{
		cli.BoolFlag{
			Name:  "debug",
			Usage: "Enable debug output in logs",
		},
		cli.StringFlag{
			Name:   "address, a", // 用于指定containerd的grpc socket, 默认值为：/run/containerd/containerd.sock
			Usage:  "Address for containerd's GRPC server",
			Value:  defaults.DefaultAddress,
			EnvVar: "CONTAINERD_ADDRESS",
		},
		cli.DurationFlag{
			Name:  "timeout", // TODO 什么超时时间？  默认值为0秒
			Usage: "Total timeout for ctr commands",
		},
		cli.DurationFlag{
			Name:  "connect-timeout", // 默认值为0秒
			Usage: "Timeout for connecting to containerd",
		},
		cli.StringFlag{
			Name:   "namespace, n", // 用于指定当前需要使用的名称空间，在containerd中，所有的资源都是名称空间级别的
			Usage:  "Namespace to use with commands",
			Value:  namespaces.Default,         // 如果没有设置，默认就是default名称空间
			EnvVar: namespaces.NamespaceEnvVar, // 我们可以通过CONTAINERD_NAMESPACE环境变量指定这个参数，而不用每次显式指定
		},
	}
	app.Commands = append([]cli.Command{
		plugins.Command,
		versionCmd.Command,
		containers.Command,
		content.Command,
		events.Command,
		images.Command,
		leases.Command,
		namespacesCmd.Command,
		pprof.Command,
		run.Command,
		snapshots.Command,
		tasks.Command,
		install.Command,
		ociCmd.Command,
		sandboxes.Command,
		info.Command,
	}, extraCmds...)
	app.Before = func(context *cli.Context) error {
		if context.GlobalBool("debug") { // 如果设置了debug参数，就直接把日志设置为debug级别的
			logrus.SetLevel(logrus.DebugLevel)
		}
		return nil
	}
	return app
}
