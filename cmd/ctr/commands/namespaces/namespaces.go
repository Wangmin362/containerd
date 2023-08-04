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

package namespaces

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/containerd/containerd/cmd/ctr/commands"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/urfave/cli"
)

// Command is the cli command for managing namespaces
var Command = cli.Command{
	Name:    "namespaces",
	Aliases: []string{"namespace", "ns"},
	Usage:   "Manage namespaces",
	Subcommands: cli.Commands{
		createCommand,    // 创建名称空间
		listCommand,      // 列出所有的名称空间
		removeCommand,    // 移除某个名称空间
		setLabelsCommand, // 为某个名称空间设置标签，或者清除某个名称空间的所有标签
	},
}

var createCommand = cli.Command{
	Name:        "create",
	Aliases:     []string{"c"},
	Usage:       "Create a new namespace",
	ArgsUsage:   "<name> [<key>=<value>]",
	Description: "create a new namespace. it must be unique",
	Action: func(context *cli.Context) error {
		// 获取当前请求的名称空间以及标签
		namespace, labels := commands.ObjectWithLabelArgs(context)
		// 名称空间必须要指定
		if namespace == "" {
			return errors.New("please specify a namespace")
		}
		client, ctx, cancel, err := commands.NewClient(context)
		if err != nil {
			return err
		}
		defer cancel()
		// 获取名称空间客户端
		namespaces := client.NamespaceService()
		// 创建名称空间
		return namespaces.Create(ctx, namespace, labels)
	},
}

// TODO 清除名称空间的标签似乎有问题呀？ 如果没有任何标签，这里应该不会做任何操作才对
var setLabelsCommand = cli.Command{
	Name:        "label",
	Usage:       "Set and clear labels for a namespace",
	ArgsUsage:   "<name> [<key>=<value>, ...]",
	Description: "set and clear labels for a namespace. empty value clears the label",
	Action: func(context *cli.Context) error {
		namespace, labels := commands.ObjectWithLabelArgs(context)
		if namespace == "" {
			return errors.New("please specify a namespace")
		}
		client, ctx, cancel, err := commands.NewClient(context)
		if err != nil {
			return err
		}
		defer cancel()
		namespaces := client.NamespaceService()
		for k, v := range labels {
			if err := namespaces.SetLabel(ctx, namespace, k, v); err != nil {
				return err
			}
		}
		return nil
	},
}

var listCommand = cli.Command{
	Name:        "list",
	Aliases:     []string{"ls"},
	Usage:       "List namespaces",
	ArgsUsage:   "[flags]",
	Description: "list namespaces",
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "quiet, q",
			Usage: "Print only the namespace name",
		},
	},
	Action: func(context *cli.Context) error {
		quiet := context.Bool("quiet")
		client, ctx, cancel, err := commands.NewClient(context)
		if err != nil {
			return err
		}
		defer cancel()
		namespaces := client.NamespaceService()
		nss, err := namespaces.List(ctx)
		if err != nil {
			return err
		}

		if quiet {
			for _, ns := range nss {
				fmt.Println(ns)
			}
			return nil
		}

		tw := tabwriter.NewWriter(os.Stdout, 1, 8, 1, ' ', 0)
		fmt.Fprintln(tw, "NAME\tLABELS\t")
		for _, ns := range nss {
			labels, err := namespaces.Labels(ctx, ns)
			if err != nil {
				return err
			}

			var labelStrings []string
			for k, v := range labels {
				labelStrings = append(labelStrings, strings.Join([]string{k, v}, "="))
			}
			sort.Strings(labelStrings)

			fmt.Fprintf(tw, "%v\t%v\t\n", ns, strings.Join(labelStrings, ","))
		}
		return tw.Flush()
	},
}

var removeCommand = cli.Command{
	Name:        "remove",
	Aliases:     []string{"rm"},
	Usage:       "Remove one or more namespaces",
	ArgsUsage:   "<name> [<name>, ...]",
	Description: "remove one or more namespaces. for now, the namespace must be empty",
	Flags: []cli.Flag{
		cli.BoolFlag{
			Name:  "cgroup,c",
			Usage: "Delete the namespace's cgroup",
		},
	},
	Action: func(context *cli.Context) error {
		var exitErr error
		client, ctx, cancel, err := commands.NewClient(context)
		if err != nil {
			return err
		}
		defer cancel()

		opts := deleteOpts(context)
		namespaces := client.NamespaceService()
		// 移除名称空间，一次性可以移除多个
		for _, target := range context.Args() {
			if err := namespaces.Delete(ctx, target, opts...); err != nil {
				if !errdefs.IsNotFound(err) {
					if exitErr == nil {
						exitErr = fmt.Errorf("unable to delete %v: %w", target, err)
					}
					log.G(ctx).WithError(err).Errorf("unable to delete %v", target)
					continue
				}

			}

			fmt.Println(target)
		}
		return exitErr
	},
}
