//go:build !windows

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
	"crypto/x509"
	"path/filepath"
)

// 用于获取/etc/containerd/certs.d目录中配置的仓库
func hostPaths(root, host string) (hosts []string) {
	// 1、此方法用于获取仓库的host
	// 2、如果当前仓库配置了端口，那么需要把端口去掉，才能取出host
	ch := hostDirectory(host)
	// 如果不相等，说明仓库配置了端口，并没有使用标准端口
	if ch != host {
		// 如果仓库地址带了端口，这里会把去掉带进去
		hosts = append(hosts, filepath.Join(root, ch))
	}

	hosts = append(hosts,
		// 如果仓库地址带了端口，这里会把端口地址带进去
		filepath.Join(root, host),
		// 从这里可以看出，/etc/containerd/certs.d目录可以配置一个_default目录用来作为默认配置
		filepath.Join(root, "_default"),
	)

	return
}

func rootSystemPool() (*x509.CertPool, error) {
	return x509.SystemCertPool()
}
