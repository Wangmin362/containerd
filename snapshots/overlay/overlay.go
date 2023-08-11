//go:build linux

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

package overlay

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/overlay/overlayutils"
	"github.com/containerd/containerd/snapshots/storage"
	"github.com/containerd/continuity/fs"
	"github.com/sirupsen/logrus"
)

// upperdirKey is a key of an optional label to each snapshot.
// This optional label of a snapshot contains the location of "upperdir" where
// the change set between this snapshot and its parent is stored.
const upperdirKey = "containerd.io/snapshot/overlay.upperdir"

// SnapshotterConfig is used to configure the overlay snapshotter instance
type SnapshotterConfig struct {
	// TODO 什么叫做异步移除？
	asyncRemove bool
	// TODO 这个参数用来干嘛的？
	upperdirLabel bool
}

// Opt is an option to configure the overlay snapshotter
type Opt func(config *SnapshotterConfig) error

// AsynchronousRemove defers removal of filesystem content until
// the Cleanup method is called. Removals will make the snapshot
// referred to by the key unavailable and make the key immediately
// available for re-use.
func AsynchronousRemove(config *SnapshotterConfig) error {
	config.asyncRemove = true
	return nil
}

// WithUpperdirLabel adds as an optional label
// "containerd.io/snapshot/overlay.upperdir". This stores the location
// of the upperdir that contains the changeset between the labelled
// snapshot and its parent.
func WithUpperdirLabel(config *SnapshotterConfig) error {
	config.upperdirLabel = true
	return nil
}

type snapshotter struct {
	// 这里的root应该就是值得overlay-snapshotter插件保存数据的位置，默认为：/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs
	root        string
	ms          *storage.MetaStore
	asyncRemove bool
	// 这个参数如果开启了，那么返回给调用方的快照标签会增加：containerd.io/snapshot/overlay.upperdir=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>/fs
	upperdirLabel bool
	indexOff      bool
	userxattr     bool // whether to enable "userxattr" mount option
}

// NewSnapshotter returns a Snapshotter which uses overlayfs. The overlayfs
// diffs are stored under the provided root. A metadata file is stored under
// the root.
func NewSnapshotter(root string, opts ...Opt) (snapshots.Snapshotter, error) {
	var config SnapshotterConfig
	for _, opt := range opts {
		if err := opt(&config); err != nil {
			return nil, err
		}
	}

	// 创建/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs目录
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	// 所谓的d_type实际上是Linux内核中的一种数据结构，用于保存某些数据。而不同的文件系统可以选择性的实现d_type特性，overlayfs文件系统
	// 是一种上层的文件系统，其底层的文件系统可以是ext4或者xfs文件系统。想要使用overlay文件系统必须开启d_type特性。
	supportsDType, err := fs.SupportsDType(root)
	if err != nil {
		return nil, err
	}
	// 如果当前的文件系统不支持d_type，就无法使用overlay文件系统
	if !supportsDType {
		return nil, fmt.Errorf("%s does not support d_type. If the backing filesystem is xfs, please reformat with ftype=1 to enable d_type support", root)
	}
	// 把某些元数据保存在/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/metadata.db文件当中
	ms, err := storage.NewMetaStore(filepath.Join(root, "metadata.db"))
	if err != nil {
		return nil, err
	}

	// 创建/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots目录
	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}
	// figure out whether "userxattr" option is recognized by the kernel && needed
	// TODO linux操作系统的扩展属性
	userxattr, err := overlayutils.NeedsUserXAttr(root)
	if err != nil {
		logrus.WithError(err).Warnf("cannot detect whether \"userxattr\" option needs to be used, assuming to be %v", userxattr)
	}

	return &snapshotter{
		root:          root,
		ms:            ms,
		asyncRemove:   config.asyncRemove,
		upperdirLabel: config.upperdirLabel,
		indexOff:      supportsIndex(),
		userxattr:     userxattr,
	}, nil
}

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
// 1、key的格式为：default/<index>/<digest>，譬如：default/4/sha256:4393f4a23174c8219b87411a6f1d20f7a7b1bcc5cd5ee2a3e8994bfc7095c614
// 2、直接从/v1/snapshots/<key>桶中读取inodes, size, kind, parent, createTime, updateTime, labels属性
func (o *snapshotter) Stat(ctx context.Context, key string) (info snapshots.Info, err error) {
	var id string
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		// key的格式为：default/<index>/<digest>，譬如：default/4/sha256:4393f4a23174c8219b87411a6f1d20f7a7b1bcc5cd5ee2a3e8994bfc7095c614
		// 直接从/v1/snapshots/<key>桶中读取inodes, size, kind, parent, createTime, updateTime, labels属性
		id, info, _, err = storage.GetInfo(ctx, key)
		return err
	}); err != nil {
		return info, err
	}

	if o.upperdirLabel {
		if info.Labels == nil {
			info.Labels = make(map[string]string)
		}
		info.Labels[upperdirKey] = o.upperPath(id)
	}
	return info, nil
}

// Update 根据快照的key，更新快照的标签信息
// 如果开启了upperdirLabel，那么会增加containerd.io/snapshot/overlay.upperdir = /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>/fs的标签
func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (newInfo snapshots.Info, err error) {
	// 开启事务
	err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		// 实际上更新快照信息只能更新快照的标签以及更新时间信息，这里更新的也只是快照的元数据信息，数据保存在boltdb当中
		// newInfo为更新之后的信息
		newInfo, err = storage.UpdateInfo(ctx, info, fieldpaths...)
		if err != nil {
			return err
		}

		if o.upperdirLabel {
			// 从元数据中读取当前快照的的id
			id, _, _, err := storage.GetInfo(ctx, newInfo.Name)
			if err != nil {
				return err
			}
			if newInfo.Labels == nil {
				newInfo.Labels = make(map[string]string)
			}
			// upperPath为：/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>/fs
			newInfo.Labels[upperdirKey] = o.upperPath(id)
		}
		return nil
	})
	return newInfo, err
}

// Usage returns the resources taken by the snapshot identified by key.
//
// For active snapshots, this will scan the usage of the overlay "diff" (aka
// "upper") directory and may take some time.
//
// For committed snapshots, the value is returned from the metadata database.
// 根据快照的key获取镜像的磁盘使用情况，譬如inodes数量，以及镜像大小
func (o *snapshotter) Usage(ctx context.Context, key string) (_ snapshots.Usage, err error) {
	var (
		usage snapshots.Usage
		info  snapshots.Info
		id    string
	)
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		// key的格式为：default/<index>/<digest>，譬如：default/4/sha256:4393f4a23174c8219b87411a6f1d20f7a7b1bcc5cd5ee2a3e8994bfc7095c614
		// 直接从/v1/snapshots/<key>桶中读取kind, id属性
		id, info, usage, err = storage.GetInfo(ctx, key)
		return err
	}); err != nil {
		return usage, err
	}

	// 如果当前快照为KindActive专改，需要从磁盘中加载usage信息
	if info.Kind == snapshots.KindActive {
		// 快照路径为：/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>/fs
		upperPath := o.upperPath(id)
		// 通过读取文件信息获取磁盘使用量
		du, err := fs.DiskUsage(ctx, upperPath)
		if err != nil {
			// TODO(stevvooe): Consider not reporting an error in this case.
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}
	return usage, nil
}

// Prepare 1、所谓的创建快照，其实仅仅是报快照的元信息保存到boltdb当中，然后在创建/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>目录
// 其中，快照的ID是通过boltdb的桶序列号生成的
// 2、Prepare创建的是KindActive类型的快照
func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

// View 1、所谓的创建快照，其实仅仅是报快照的元信息保存到boltdb当中，然后在创建/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>目录
// 其中，快照的ID是通过boltdb的桶序列号生成的
// 2、View创建的是KindActive类型的快照
func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
// TODO 根据快照的key获取当前快照信息，并返回当前快照的挂载路径（包括parent）
func (o *snapshotter) Mounts(ctx context.Context, key string) (_ []mount.Mount, err error) {
	var s storage.Snapshot
	// 开启事务，根据key获取当前快照的信息
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		// 根据快照的key，获取当前快照信息，其中把包括快照的状态，ID以及当前快照的所有Parent ID
		s, err = storage.GetSnapshot(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get active mount: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return o.mounts(s), nil
}

// Commit 实际上就是把处于KindActive的快照修改为KindCommitted状态，然后更新元数据信息
func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	// 开启事务
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		// grab the existing id
		// 根据快照的key获取快照的ID信息
		id, _, _, err := storage.GetInfo(ctx, key)
		if err != nil {
			return err
		}

		// 统计/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>/fs目录的磁盘使用信息
		usage, err := fs.DiskUsage(ctx, o.upperPath(id))
		if err != nil {
			return err
		}

		// 实际上就是把处于KindActive的快照修改为KindCommitted状态，然后更新元数据信息
		if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
			return fmt.Errorf("failed to commit snapshot %s: %w", key, err)
		}
		return nil
	})
}

// Remove abandons the snapshot identified by key. The snapshot will
// immediately become unavailable and unrecoverable. Disk space will
// be freed up on the next call to `Cleanup`.
// 1、根据快照的key，删除镜像
// 2、实际上镜像数据有两个部分，一部分称之为元数据，保存在boltdb当中，主要是镜像的一些属性信息，譬如kind, labels, size, inodes, id, leabels
// 等等；另外一部分则是进项的正式数据，以文件的形式（bolb）保存在磁盘上。因此删除快照的时候，需要删除两个部分，也即是元数据这真实数据。
// 3、根据用户参数的设置，真实数据的删除可能是异步删除，这里只删除元数据
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	var removals []string
	// Remove directories after the transaction is closed, failures must not
	// return error since the transaction is committed with the removal
	// key no longer available.
	defer func() {
		if err == nil {
			// 如果是同步删除，那么需要在退出的时候把删除的快照同时删除
			for _, dir := range removals {
				if err := os.RemoveAll(dir); err != nil {
					log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
				}
			}
		}
	}()
	// 开启事务
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		// 删除/v1/snapshots/<key>桶，清除数据
		_, _, err = storage.Remove(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to remove snapshot %s: %w", key, err)
		}

		// 如果没有开启异步移除，那么就需要立马删除当前快照
		if !o.asyncRemove {
			// 目的：为了获取需要删除的镜像的路径
			// 原理：元数据保存在boltdb当中，对比元数据和目录，如果元数据中不存在，但是存在这个目录，那么这个目录下的快照就需要被删除。
			removals, err = o.getCleanupDirectories(ctx)
			if err != nil {
				return fmt.Errorf("unable to get directories for removal: %w", err)
			}
		}
		return nil
	})
}

// Walk the snapshots.
// 遍历所有的快照，并按照调用方指定的选择器过滤出调用方需要的快照
func (o *snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	// 开启事务
	return o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		if o.upperdirLabel {
			return storage.WalkInfo(ctx, func(ctx context.Context, info snapshots.Info) error {
				id, _, _, err := storage.GetInfo(ctx, info.Name)
				if err != nil {
					return err
				}
				if info.Labels == nil {
					info.Labels = make(map[string]string)
				}
				info.Labels[upperdirKey] = o.upperPath(id)
				return fn(ctx, info)
			}, fs...)
		}
		return storage.WalkInfo(ctx, fn, fs...)
	})
}

// Cleanup cleans up disk resources from removed or abandoned snapshots
// 目的：清理无用的镜像，这些镜像实际上已经被删除
// 原理：元数据保存在boltdb当中，对比元数据和目录，如果元数据中不存在，但是存在这个目录，那么这个目录下的快照就需要被删除。
func (o *snapshotter) Cleanup(ctx context.Context) error {
	cleanup, err := o.cleanupDirectories(ctx)
	if err != nil {
		return err
	}

	for _, dir := range cleanup {
		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
		}
	}

	return nil
}

// 目的：为了获取需要删除的镜像
// 原理：元数据保存在boltdb当中，对比元数据和目录，如果元数据中不存在，但是存在这个目录，那么这个目录下的快照就需要被删除。
func (o *snapshotter) cleanupDirectories(ctx context.Context) (_ []string, err error) {
	var cleanupDirs []string
	// Get a write transaction to ensure no other write transaction can be entered
	// while the cleanup is scanning.
	if err := o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		cleanupDirs, err = o.getCleanupDirectories(ctx)
		return err
	}); err != nil {
		return nil, err
	}
	return cleanupDirs, nil
}

// 目的：为了获取需要删除的镜像
// 原理：元数据保存在boltdb当中，对比元数据和目录，如果元数据中不存在，但是存在这个目录，那么这个目录下的快照就需要被删除。
func (o *snapshotter) getCleanupDirectories(ctx context.Context) ([]string, error) {
	// 读取所有快照的ID映射关系放入到map当中，map的key为快照ID，而value为快照的Key
	ids, err := storage.IDMap(ctx)
	if err != nil {
		return nil, err
	}

	snapshotDir := filepath.Join(o.root, "snapshots")
	fd, err := os.Open(snapshotDir)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	// 读取/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots目录下所有的目录
	dirs, err := fd.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	cleanup := []string{}
	for _, d := range dirs {
		// 如果元数据中存在，但是却有这个目录，说明这个目录中的快照应该删除
		if _, ok := ids[d]; ok {
			continue
		}
		cleanup = append(cleanup, filepath.Join(snapshotDir, d))
	}

	return cleanup, nil
}

// 所谓的创建快照，其实仅仅是报快照的元信息保存到boltdb当中，然后在创建/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>目录
// 其中，快照的ID是通过boltdb的桶序列号生成的
func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	var (
		s        storage.Snapshot
		td, path string
	)

	defer func() {
		if err != nil {
			if td != "" {
				// 如果创建快照失败，那么需要移除临时目录
				if err1 := os.RemoveAll(td); err1 != nil {
					log.G(ctx).WithError(err1).Warn("failed to cleanup temp snapshot directory")
				}
			}
			if path != "" {
				if err1 := os.RemoveAll(path); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", path).Error("failed to reclaim snapshot directory, directory may need removal")
					err = fmt.Errorf("failed to remove path: %v: %w", err1, err)
				}
			}
		}
	}()

	// 开启事务
	if err := o.ms.WithTransaction(ctx, true, func(ctx context.Context) (err error) {
		// /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots
		snapshotDir := filepath.Join(o.root, "snapshots")
		// 在/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/目录中创建目录，然后在临时目录中创建fs目录以及work目录
		td, err = o.prepareDirectory(ctx, snapshotDir, kind)
		if err != nil {
			return fmt.Errorf("failed to create prepare snapshot dir: %w", err)
		}

		// 所谓的创建快照，其实仅仅是保存快照的元信息到boltdb当中
		s, err = storage.CreateSnapshot(ctx, kind, key, parent, opts...)
		if err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		if len(s.ParentIDs) > 0 {
			st, err := os.Stat(o.upperPath(s.ParentIDs[0]))
			if err != nil {
				return fmt.Errorf("failed to stat parent: %w", err)
			}

			stat := st.Sys().(*syscall.Stat_t)
			// 修改/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/new-xxx/fs目录的uid以及gid
			if err := os.Lchown(filepath.Join(td, "fs"), int(stat.Uid), int(stat.Gid)); err != nil {
				return fmt.Errorf("failed to chown: %w", err)
			}
		}

		// path=/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>
		path = filepath.Join(snapshotDir, s.ID)
		// 把临时目录重命名为/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>
		if err = os.Rename(td, path); err != nil {
			return fmt.Errorf("failed to rename: %w", err)
		}
		td = ""

		return nil
	}); err != nil {
		return nil, err
	}

	return o.mounts(s), nil
}

// snapshotDir = /var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots
// 在/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/目录中创建目录，然后在临时目录中创建fs目录以及work目录
func (o *snapshotter) prepareDirectory(ctx context.Context, snapshotDir string, kind snapshots.Kind) (string, error) {
	// 在/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots目录中创建一个零食目录，目录pattern为：new-
	td, err := os.MkdirTemp(snapshotDir, "new-")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}

	// 创建/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/new-*/fs目录
	if err := os.Mkdir(filepath.Join(td, "fs"), 0755); err != nil {
		return td, err
	}

	// 如果当前快照类型为KindActive，那么创建/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/new-*/work目录
	if kind == snapshots.KindActive {
		if err := os.Mkdir(filepath.Join(td, "work"), 0711); err != nil {
			return td, err
		}
	}

	return td, nil
}

func (o *snapshotter) mounts(s storage.Snapshot) []mount.Mount {
	// 一个快照的没有Parent，说明是最基础的那一层
	if len(s.ParentIDs) == 0 {
		// if we only have one layer/no parents then just return a bind mount as overlay
		// will not work
		roFlag := "rw"
		if s.Kind == snapshots.KindView {
			roFlag = "ro"
		}

		return []mount.Mount{
			{
				Source: o.upperPath(s.ID),
				Type:   "bind",
				Options: []string{
					roFlag,
					"rbind",
				},
			},
		}
	}
	var options []string

	// set index=off when mount overlayfs
	if o.indexOff {
		options = append(options, "index=off")
	}

	if o.userxattr {
		options = append(options, "userxattr")
	}

	if s.Kind == snapshots.KindActive {
		options = append(options,
			fmt.Sprintf("workdir=%s", o.workPath(s.ID)),
			fmt.Sprintf("upperdir=%s", o.upperPath(s.ID)),
		)
	} else if len(s.ParentIDs) == 1 {
		return []mount.Mount{
			{
				Source: o.upperPath(s.ParentIDs[0]),
				Type:   "bind",
				Options: []string{
					"ro",
					"rbind",
				},
			},
		}
	}

	parentPaths := make([]string, len(s.ParentIDs))
	for i := range s.ParentIDs {
		parentPaths[i] = o.upperPath(s.ParentIDs[i])
	}

	options = append(options, fmt.Sprintf("lowerdir=%s", strings.Join(parentPaths, ":")))
	return []mount.Mount{
		{
			Type:    "overlay",
			Source:  "overlay",
			Options: options,
		},
	}

}

// 获取/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>/fs路径
func (o *snapshotter) upperPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "fs")
}

func (o *snapshotter) workPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "work")
}

// Close closes the snapshotter
// TODO 什么叫做关闭快照器？
func (o *snapshotter) Close() error {
	return o.ms.Close()
}

// supportsIndex checks whether the "index=off" option is supported by the kernel.
// 判断当前操作操作系统是否支持索引
func supportsIndex() bool {
	if _, err := os.Stat("/sys/module/overlay/parameters/index"); err == nil {
		return true
	}
	return false
}
