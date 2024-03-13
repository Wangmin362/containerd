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

package plugin

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/metadata"
	"github.com/containerd/containerd/pkg/timeout"
	"github.com/containerd/containerd/plugin"
	"github.com/containerd/containerd/snapshots"

	bolt "go.etcd.io/bbolt"
)

const (
	boltOpenTimeout = "io.containerd.timeout.bolt.open"
)

func init() {
	// 这里将默认的超时时间设置为0，其实containerd的默认配置文件中也是设置为0，但是用户可以更改这个配置
	timeout.Set(boltOpenTimeout, 0) // set to 0 means to wait indefinitely for bolt.Open
}

// BoltConfig defines the configuration values for the bolt plugin, which is
// loaded here, rather than back registered in the metadata package.
type BoltConfig struct {
	// ContentSharingPolicy sets the sharing policy for content between
	// namespaces.
	//
	// The default mode "shared" will make blobs available in all
	// namespaces once it is pulled into any namespace. The blob will be pulled
	// into the namespace if a writer is opened with the "Expected" digest that
	// is already present in the backend.
	//
	// The alternative mode, "isolated" requires that clients prove they have
	// access to the content by providing all of the content to the ingest
	// before the blob is added to the namespace.
	//
	// Both modes share backing data, while "shared" will reduce total
	// bandwidth across namespaces, at the cost of allowing access to any blob
	// just by knowing its digest.
	// 1、内容共享配置 所谓的内容共享，其实就是下载的blob是否是名称空间隔离的，在containerd中，可以通过名称隔离任意资源。
	// 而不同名称空间也可以下载相同的镜像。默认情况下，内容共享策略的模式为共享，也就是说不同名称空间的blob资源是共享的
	// 并不会为每个空城空间单独下载镜像。显然，这样设计也是有道理的，对于镜像这资源来说，完全没有必要每个名称空间都下载一份。
	// 完全可以所有名称空间共享镜像blob，每当其它名称空间需要这个镜像的时候，只需要在通过metadata插件在此名称空间中
	// 保存一份元数据即可，不需要重新下载镜像。
	// 2、内容共享策略也可以配置为isolated模式，即镜像资源也是隔离的。此时containerd会为每个名称空间都下载相同的镜像blob
	ContentSharingPolicy string `toml:"content_sharing_policy"`
}

const (
	// SharingPolicyShared represents the "shared" sharing policy
	SharingPolicyShared = "shared"
	// SharingPolicyIsolated represents the "isolated" sharing policy
	SharingPolicyIsolated = "isolated"
)

// Validate validates if BoltConfig is valid
func (bc *BoltConfig) Validate() error {
	switch bc.ContentSharingPolicy {
	case SharingPolicyShared, SharingPolicyIsolated:
		return nil
	default:
		return fmt.Errorf("unknown policy: %s: %w", bc.ContentSharingPolicy, errdefs.ErrInvalidArgument)
	}
}

// TODO Q：元数据插件对外提供了什么能力？
// A：从元数据插件对外暴露的方法来看，主要是提供了这么几类服务：其一内容存储服务，用于存储镜像blob; 其二为垃圾回收服务
// 其三为快照服务  其四为元数据服务，也就是把需要的数据可以存储在boltdb当中
func init() {
	// 注册插件
	plugin.Register(&plugin.Registration{
		Type: plugin.MetadataPlugin, // 元数据插件类型，这个插件是非常底层的类型，用于通过boltdb存储元数据
		ID:   "bolt",                // 那么元数据插件的根目录为：/var/lib/containerd/io.containerd.metadata.v1.bolt
		Requires: []plugin.Type{
			plugin.ContentPlugin,  // 元数据插件依赖ContentPlugin用于存储真实的数据，譬如blob, ingest
			plugin.SnapshotPlugin, // TODO 为什么元数据插件还依赖快照插件？
		},
		Config: &BoltConfig{ // 元数据插件的配置
			ContentSharingPolicy: SharingPolicyShared,
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			// 创建/var/lib/containerd/io.containerd.metadata.v1.bolt目录
			if err := os.MkdirAll(ic.Root, 0711); err != nil {
				return nil, err
			}
			// 获取内容插件，因为元数据插件在存储数据的时候还需要存储真正的数据，并非只存储元数据
			cs, err := ic.Get(plugin.ContentPlugin)
			if err != nil {
				return nil, err
			}

			// 获取所有的快照插件 TODO 快照插件的具体作用还需要分析
			snapshottersRaw, err := ic.GetByType(plugin.SnapshotPlugin)
			if err != nil {
				return nil, err
			}

			snapshotters := make(map[string]snapshots.Snapshotter)
			for name, sn := range snapshottersRaw {
				// 1、获取快照插件实例
				// 2、注意：快照插件的实例化并非在这里做的，而是在containerd初始化的时候完成的各个插件的实例化
				// 3、如果快照实例化出错，那么直接忽略这个快照插件
				sn, err := sn.Instance()
				if err != nil {
					// 如果当前快照插件实例化的时候出错，并且错误不是跳过此插件的错误，就打印警告
					if !plugin.IsSkipPlugin(err) {
						log.G(ic.Context).WithError(err).
							Warnf("could not use snapshotter %v in metadata plugin", name)
					}
					continue
				}
				snapshotters[name] = sn.(snapshots.Snapshotter)
			}

			shared := true
			// 导出元数据插件的内容共享策略，默认情况下就是shared
			ic.Meta.Exports["policy"] = SharingPolicyShared
			if cfg, ok := ic.Config.(*BoltConfig); ok {
				if cfg.ContentSharingPolicy != "" {
					// 校验共享策略值，只支持shared, isolate这两种模式
					if err := cfg.Validate(); err != nil {
						return nil, err
					}
					// 如果设置为了隔离策略，说明对于镜像层这样的资源，所有名称空间都是隔离的。不同的名称空间如果需要相同的
					// 镜像，就需要重新进行下载
					if cfg.ContentSharingPolicy == SharingPolicyIsolated {
						ic.Meta.Exports["policy"] = SharingPolicyIsolated
						shared = false
					}

					log.G(ic.Context).WithField("policy", cfg.ContentSharingPolicy).Info("metadata content store policy set")
				}
			}

			// /var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db文件
			path := filepath.Join(ic.Root, "meta.db")
			// path = /var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db
			ic.Meta.Exports["path"] = path

			// 使用boltdb的默认配置，下面会进行修改
			options := *bolt.DefaultOptions
			// Reading bbolt's freelist sometimes fails when the file has a data corruption.
			// Disabling freelist sync reduces the chance of the breakage.
			// https://github.com/etcd-io/bbolt/pull/1
			// https://github.com/etcd-io/bbolt/pull/6
			options.NoFreelistSync = true
			// Without the timeout, bbolt.Open would block indefinitely due to flock(2).
			// 获取超时时间，用户可以通过containerd配置文件的io.containerd.timeout.bolt.open参数修改
			options.Timeout = timeout.Get(boltOpenTimeout)

			doneCh := make(chan struct{})
			go func() {
				t := time.NewTimer(10 * time.Second)
				defer t.Stop()
				select {
				case <-t.C:
					// 如果打印这行日志，说明元数据插件在10秒钟之内都没有打开/var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db文件
					log.G(ic.Context).WithField("plugin", "bolt").Warn("waiting for response from boltdb open")
				case <-doneCh:
					return
				}
			}()
			// 数据保存在/var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db文件当中
			db, err := bolt.Open(path, 0644, &options)
			close(doneCh)
			if err != nil {
				return nil, err
			}

			dbopts := []metadata.DBOpt{
				metadata.WithEventsPublisher(ic.Events),
			}

			if !shared {
				// 如果不是共享的，这里在存储数据的时候就需要在元数据上说明是非共享的
				dbopts = append(dbopts, metadata.WithPolicyIsolated)
			}

			// 从这里可以看出，元数据插件的核心揪心boltdb，并且这里肯定需要返回插件的引用，其它插件才能获取到元数据插件的数据
			mdb := metadata.NewDB(db, cs.(content.Store), snapshotters, dbopts...)
			// 判断当前boltdb存的数据是否是正确的版本，如果不是，就需要迁移数据  TODO 迁移数据的细节还需要分析
			if err := mdb.Init(ic.Context); err != nil {
				return nil, err
			}
			return mdb, nil
		},
	})
}
