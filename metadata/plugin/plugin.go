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
	// TODO 这个配置是干嘛用的？
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

func init() {
	plugin.Register(&plugin.Registration{
		Type: plugin.MetadataPlugin,
		ID:   "bolt",
		Requires: []plugin.Type{
			plugin.ContentPlugin,
			plugin.SnapshotPlugin,
		},
		Config: &BoltConfig{
			ContentSharingPolicy: SharingPolicyShared,
		},
		InitFn: func(ic *plugin.InitContext) (interface{}, error) {
			// 创建/var/lib/containerd/io.containerd.metadata.v1.bolt目录
			if err := os.MkdirAll(ic.Root, 0711); err != nil {
				return nil, err
			}
			// 获取内容插件
			cs, err := ic.Get(plugin.ContentPlugin)
			if err != nil {
				return nil, err
			}

			// 获取快照插件 TODO 快照插件的具体作用还需要分析
			snapshottersRaw, err := ic.GetByType(plugin.SnapshotPlugin)
			if err != nil {
				return nil, err
			}

			snapshotters := make(map[string]snapshots.Snapshotter)
			for name, sn := range snapshottersRaw {
				// 实例化快照插件
				sn, err := sn.Instance()
				if err != nil {
					if !plugin.IsSkipPlugin(err) {
						log.G(ic.Context).WithError(err).
							Warnf("could not use snapshotter %v in metadata plugin", name)
					}
					continue
				}
				snapshotters[name] = sn.(snapshots.Snapshotter)
			}

			shared := true
			// TODO 导出的变量有啥用？
			ic.Meta.Exports["policy"] = SharingPolicyShared
			if cfg, ok := ic.Config.(*BoltConfig); ok {
				if cfg.ContentSharingPolicy != "" {
					if err := cfg.Validate(); err != nil {
						return nil, err
					}
					if cfg.ContentSharingPolicy == SharingPolicyIsolated {
						ic.Meta.Exports["policy"] = SharingPolicyIsolated
						shared = false
					}

					log.G(ic.Context).WithField("policy", cfg.ContentSharingPolicy).Info("metadata content store policy set")
				}
			}

			// /var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db文件
			path := filepath.Join(ic.Root, "meta.db")
			ic.Meta.Exports["path"] = path

			options := *bolt.DefaultOptions
			// Reading bbolt's freelist sometimes fails when the file has a data corruption.
			// Disabling freelist sync reduces the chance of the breakage.
			// https://github.com/etcd-io/bbolt/pull/1
			// https://github.com/etcd-io/bbolt/pull/6
			options.NoFreelistSync = true
			// Without the timeout, bbolt.Open would block indefinitely due to flock(2).
			options.Timeout = timeout.Get(boltOpenTimeout)

			doneCh := make(chan struct{})
			go func() {
				t := time.NewTimer(10 * time.Second)
				defer t.Stop()
				select {
				case <-t.C:
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
				dbopts = append(dbopts, metadata.WithPolicyIsolated)
			}

			// 从这里可以看出，元数据插件的核心揪心boltdb，并且这里肯定需要返回插件的引用，其它插件才能获取到元数据插件的数据
			mdb := metadata.NewDB(db, cs.(content.Store), snapshotters, dbopts...)
			// 判断当前boltdb存的数据是否是正确的版本，如果不是，就需要迁移数据
			if err := mdb.Init(ic.Context); err != nil {
				return nil, err
			}
			return mdb, nil
		},
	})
}
