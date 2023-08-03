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

package opt

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/containerd/containerd/plugin"
)

// Config for the opt manager
type Config struct {
	// Path for the opt directory
	Path string `toml:"path"`
}

// 主要作用如下：
// 1、创建/opt/containerd/bin目录，然后把此目录追加到PATH环境变量当中
// 2、创建/opt/containerd/lib目录，然后把此目录追加到LD_LIBRARY_PATH环境变量当中
func init() {
	plugin.Register(&plugin.Registration{
		Type: plugin.InternalPlugin,
		ID:   "opt",
		Config: &Config{
			Path: defaultPath, // 路径为：/opt/containerd
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			path := ic.Config.(*Config).Path
			ic.Meta.Exports["path"] = path
			bin := filepath.Join(path, "bin")
			// 创建/opt/containerd/bin目录
			if err := os.MkdirAll(bin, 0711); err != nil {
				return nil, err
			}
			// PATH = /opt/containerd/bin:$PATH
			// 这里实际上就是修改PATH环境变量，把/opt/containerd/bin路径添加到PATH变量当中
			// TODO 为什么containerd起来之后，通过echo $PATH没有看到这里添加的环境变量
			if err := os.Setenv("PATH", fmt.Sprintf("%s%c%s", bin, os.PathListSeparator, os.Getenv("PATH"))); err != nil {
				return nil, fmt.Errorf("set binary image directory in path %s: %w", bin, err)
			}

			lib := filepath.Join(path, "lib")
			// 创建/opt/containerd/lib目录
			if err := os.MkdirAll(lib, 0711); err != nil {
				return nil, err
			}
			// 设置环境变量 LD_LIBRARY_PATH = /opt/containerd/lib:$LD_LIBRARY_PATH
			// 实际上就是在给LD_LIBRARY_PATH环境变量添加/opt/container/lib路径
			if err := os.Setenv("LD_LIBRARY_PATH", fmt.Sprintf("%s%c%s", lib, os.PathListSeparator, os.Getenv("LD_LIBRARY_PATH"))); err != nil {
				return nil, fmt.Errorf("set binary lib directory in path %s: %w", lib, err)
			}
			return &manager{}, nil
		},
	})
}

type manager struct {
}
