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

package main

import (
	"crypto"
	"fmt"
	"os"

	"github.com/containerd/containerd/cmd/ctr/app"
	"github.com/containerd/containerd/pkg/hasher"
	"github.com/containerd/containerd/pkg/seed" //nolint:staticcheck // Global math/rand seed is deprecated, but still used by external dependencies
	"github.com/urfave/cli"
)

var pluginCmds = []cli.Command{}

func init() {
	//nolint:staticcheck // Global math/rand seed is deprecated, but still used by external dependencies
	seed.WithTimeAndRand()
	// 注册SHA256摘要算法  TODO 可以看成是golang单例模式的一种，使用全局变量保存此摘要算法，全局只会实例化一个
	// TODO 简单的看了一下这个库的实现，这个库实现的sha256并没有加锁，为什么不需要考虑线程安全？
	crypto.RegisterHash(crypto.SHA256, hasher.NewSHA256)
}

func main() {
	ctr := app.New() // 实例化应用，其实就是ctr命令行工具
	ctr.Commands = append(ctr.Commands, pluginCmds...)
	if err := ctr.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "ctr: %s\n", err)
		// 只要出错，就退出程序，退出码为1
		os.Exit(1)
	}

	// 如果正常返回的话，退出码为0
}
