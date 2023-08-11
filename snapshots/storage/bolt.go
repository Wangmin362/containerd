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

package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/filters"
	"github.com/containerd/containerd/metadata/boltutil"
	"github.com/containerd/containerd/snapshots"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketKeyStorageVersion = []byte("v1")
	bucketKeySnapshot       = []byte("snapshots")
	bucketKeyParents        = []byte("parents")

	bucketKeyID     = []byte("id")
	bucketKeyParent = []byte("parent")
	bucketKeyKind   = []byte("kind")
	bucketKeyInodes = []byte("inodes")
	bucketKeySize   = []byte("size")

	// ErrNoTransaction is returned when an operation is attempted with
	// a context which is not inside of a transaction.
	ErrNoTransaction = errors.New("no transaction in context")
)

// parentKey returns a composite key of the parent and child identifiers. The
// parts of the key are separated by a zero byte.
// 返回一个联合key
func parentKey(parent, child uint64) []byte {
	b := make([]byte, binary.Size([]uint64{parent, child})+1)
	i := binary.PutUvarint(b, parent)
	j := binary.PutUvarint(b[i+1:], child)
	return b[0 : i+j+1]
}

// parentPrefixKey returns the parent part of the composite key with the
// zero byte separator.
func parentPrefixKey(parent uint64) []byte {
	b := make([]byte, binary.Size(parent)+1)
	i := binary.PutUvarint(b, parent)
	return b[0 : i+1]
}

// getParentPrefix returns the first part of the composite key which
// represents the parent identifier.
func getParentPrefix(b []byte) uint64 {
	parent, _ := binary.Uvarint(b)
	return parent
}

// GetInfo returns the snapshot Info directly from the metadata. Requires a
// context with a storage transaction.
// key的格式为：default/<index>/<digest>，譬如：default/4/sha256:4393f4a23174c8219b87411a6f1d20f7a7b1bcc5cd5ee2a3e8994bfc7095c614
// 直接从/v1/snapshots/<key>桶中读取inodes, size, kind, parent, createTime, updateTime, labels属性
func GetInfo(ctx context.Context, key string) (string, snapshots.Info, snapshots.Usage, error) {
	var (
		id uint64
		su snapshots.Usage
		si = snapshots.Info{
			Name: key,
		}
	)
	err := withSnapshotBucket(ctx, key, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		// 直接从/v1/snapshot/defalut/<index>/<digest>桶中读取inodes, size属性
		getUsage(bkt, &su)
		// 直接从/v1/snapshot/defalut/<index>/<digest>桶中读取kind, parent, createTime, updateTime, labels属性
		return readSnapshot(bkt, &id, &si)
	})
	if err != nil {
		return "", snapshots.Info{}, snapshots.Usage{}, err
	}

	return fmt.Sprintf("%d", id), si, su, nil
}

// UpdateInfo updates an existing snapshot info's data
// 实际上更新快照信息只能更新快照的标签以及更新时间信息，这里更新的也只是快照的元数据信息，数据保存在boltdb当中
func UpdateInfo(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	updated := snapshots.Info{
		Name: info.Name,
	}
	// 获取/v1/snapshots以及/v1/parents桶，并传给func
	err := withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		// 获取/v1/snapshots/<key>桶，这里的key的格式为：default/<index>/<digest>
		sbkt := bkt.Bucket([]byte(info.Name))
		if sbkt == nil {
			return fmt.Errorf("snapshot does not exist: %w", errdefs.ErrNotFound)
		}
		// 从/v1/snapshots/<key>桶中读取kind, parent, labels, createTime, updateTime
		if err := readSnapshot(sbkt, nil, &updated); err != nil {
			return err
		}

		if len(fieldpaths) > 0 {
			for _, path := range fieldpaths {
				if strings.HasPrefix(path, "labels.") {
					if updated.Labels == nil {
						updated.Labels = map[string]string{}
					}

					key := strings.TrimPrefix(path, "labels.")
					updated.Labels[key] = info.Labels[key]
					continue
				}

				switch path {
				case "labels":
					updated.Labels = info.Labels
				default:
					return fmt.Errorf("cannot update %q field on snapshot %q: %w", path, info.Name, errdefs.ErrInvalidArgument)
				}
			}
		} else {
			// Set mutable fields
			updated.Labels = info.Labels
		}
		updated.Updated = time.Now().UTC()
		if err := boltutil.WriteTimestamps(sbkt, updated.Created, updated.Updated); err != nil {
			return err
		}

		return boltutil.WriteLabels(sbkt, updated.Labels)
	})
	if err != nil {
		return snapshots.Info{}, err
	}
	return updated, nil
}

// WalkInfo iterates through all metadata Info for the stored snapshots and
// calls the provided function for each. Requires a context with a storage
// transaction.
// 遍历所有的快照，并按照调用方指定的选择器过滤出调用方需要的快照
func WalkInfo(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	filter, err := filters.ParseAll(fs...)
	if err != nil {
		return err
	}
	// TODO: allow indexes (name, parent, specific labels)
	// 获取/v1/snapshots以及/v1/parents桶，并传给func
	return withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		return bkt.ForEach(func(k, v []byte) error {
			// skip non buckets
			if v != nil {
				return nil
			}
			var (
				sbkt = bkt.Bucket(k)
				si   = snapshots.Info{
					Name: string(k),
				}
			)
			if err := readSnapshot(sbkt, nil, &si); err != nil {
				return err
			}
			if !filter.Match(adaptSnapshot(si)) {
				return nil
			}

			return fn(ctx, si)
		})
	})
}

// GetSnapshot returns the metadata for the active or view snapshot transaction
// referenced by the given key. Requires a context with a storage transaction.
// 根据快照的key，获取当前快照信息，其中把包括快照的状态，ID以及当前快照的所有Parent ID
func GetSnapshot(ctx context.Context, key string) (s Snapshot, err error) {
	// 获取/v1/snapshots以及/v1/parents桶，并传给func
	err = withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		// 获取/v1/snapshots/<key>桶，key的格式为：default/<id>/<digest>
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return fmt.Errorf("snapshot does not exist: %w", errdefs.ErrNotFound)
		}

		// 获取当前快照的id
		s.ID = fmt.Sprintf("%d", readID(sbkt))
		// 获取当前快照的kind
		s.Kind = readKind(sbkt)

		// 如果当前的状态既不是KindActive也不是KindView，就直接报错
		if s.Kind != snapshots.KindActive && s.Kind != snapshots.KindView {
			return fmt.Errorf("requested snapshot %v not active or view: %w", key, errdefs.ErrFailedPrecondition)
		}

		// 获取当前快照的parent的key
		if parentKey := sbkt.Get(bucketKeyParent); len(parentKey) > 0 {
			// 获取/v1/snapshots/<parent-key>桶,parent-key的格式为：default/<id>/<digest>
			spbkt := bkt.Bucket(parentKey)
			if spbkt == nil {
				return fmt.Errorf("parent does not exist: %w", errdefs.ErrNotFound)
			}

			// 获取快照的所有parent的ID
			s.ParentIDs, err = parents(bkt, spbkt, readID(spbkt))
			if err != nil {
				return fmt.Errorf("failed to get parent chain: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return Snapshot{}, err
	}

	return
}

// CreateSnapshot inserts a record for an active or view snapshot with the provided parent.
// 所谓的创建快照，其实仅仅是保存快照的元信息到boltdb当中
func CreateSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts ...snapshots.Opt) (s Snapshot, err error) {
	// 只能接受KindActive以及KindView类型的快照创建
	switch kind {
	case snapshots.KindActive, snapshots.KindView:
	default:
		return Snapshot{}, fmt.Errorf("snapshot type %v invalid; only snapshots of type Active or View can be created: %w", kind, errdefs.ErrInvalidArgument)
	}
	var base snapshots.Info
	// 这里的opt，目前仅仅支持快照的label信息
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			return Snapshot{}, err
		}
	}

	// 创建/v1/snapshots/parents桶，被传给func
	err = createBucketIfNotExists(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		var (
			spbkt *bolt.Bucket
		)
		// 如果当前快照有parent
		if parent != "" {
			// 创建/v1/snapshots/<parent-key>桶
			spbkt = bkt.Bucket([]byte(parent))
			if spbkt == nil {
				return fmt.Errorf("missing parent %q bucket: %w", parent, errdefs.ErrNotFound)
			}

			// parent快照的类型必须是KindCommitted
			if readKind(spbkt) != snapshots.KindCommitted {
				return fmt.Errorf("parent %q is not committed snapshot: %w", parent, errdefs.ErrInvalidArgument)
			}
		}
		// 创建创建/v1/snapshots/<key>桶
		sbkt, err := bkt.CreateBucket([]byte(key))
		if err != nil {
			if err == bolt.ErrBucketExists {
				err = fmt.Errorf("snapshot %v: %w", key, errdefs.ErrAlreadyExists)
			}
			return err
		}

		// 获取桶的下一个序列号，每用一次，增加一
		id, err := bkt.NextSequence()
		if err != nil {
			return fmt.Errorf("unable to get identifier for snapshot %q: %w", key, err)
		}

		t := time.Now().UTC()
		si := snapshots.Info{
			Parent:  parent,
			Kind:    kind,
			Labels:  base.Labels,
			Created: t,
			Updated: t,
		}
		// 保存快照的ID, Kind, Parent, Label信息到/v1/snapshots/<key>桶中
		if err := putSnapshot(sbkt, id, si); err != nil {
			return err
		}

		// 如果父快照的桶不为空
		if spbkt != nil {
			// 读取父快照的ID
			pid := readID(spbkt)

			// Store a backlink from the key to the parent. Store the snapshot name
			// as the value to allow following the backlink to the snapshot value.
			// 在/v1/parents/<child-parent-composite>中存放信息
			if err := pbkt.Put(parentKey(pid, id), []byte(key)); err != nil {
				return fmt.Errorf("failed to write parent link for snapshot %q: %w", key, err)
			}

			s.ParentIDs, err = parents(bkt, spbkt, pid)
			if err != nil {
				return fmt.Errorf("failed to get parent chain for snapshot %q: %w", key, err)
			}
		}

		s.ID = fmt.Sprintf("%d", id)
		s.Kind = kind
		return nil
	})
	if err != nil {
		return Snapshot{}, err
	}

	return
}

// Remove removes a snapshot from the metastore. The string identifier for the
// snapshot is returned as well as the kind. The provided context must contain a
// writable transaction.
func Remove(ctx context.Context, key string) (string, snapshots.Kind, error) {
	var (
		id uint64
		si snapshots.Info
	)

	// 获取/v1/snapshots以及/v1/parents桶，并传给func
	if err := withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		// 获取/v1/snapshots/<key>桶
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return fmt.Errorf("snapshot %v: %w", key, errdefs.ErrNotFound)
		}

		// 直接从/v1/snapshot/<key>桶中读取kind, parent, createTime, updateTime, labels属性
		if err := readSnapshot(sbkt, &id, &si); err != nil {
			return fmt.Errorf("failed to read snapshot %s: %w", key, err)
		}

		// 如果当前存在/v1/parents桶
		if pbkt != nil {
			// TODO 这里是在干嘛？
			k, _ := pbkt.Cursor().Seek(parentPrefixKey(id))
			// TODO 这里是在判断什么？
			if getParentPrefix(k) == id {
				return fmt.Errorf("cannot remove snapshot with child: %w", errdefs.ErrFailedPrecondition)
			}

			// 如果当前快照有父快照
			if si.Parent != "" {
				// 获取/v1/snapshots/<parent-key>桶
				spbkt := bkt.Bucket([]byte(si.Parent))
				if spbkt == nil {
					return fmt.Errorf("snapshot %v: %w", key, errdefs.ErrNotFound)
				}

				// TODO 删除key
				if err := pbkt.Delete(parentKey(readID(spbkt), id)); err != nil {
					return fmt.Errorf("failed to delete parent link: %w", err)
				}
			}
		}

		// 移除/v1/snapshots/<key>桶
		if err := bkt.DeleteBucket([]byte(key)); err != nil {
			return fmt.Errorf("failed to delete snapshot: %w", err)
		}

		return nil
	}); err != nil {
		return "", 0, err
	}

	return fmt.Sprintf("%d", id), si.Kind, nil
}

// CommitActive renames the active snapshot transaction referenced by `key`
// as a committed snapshot referenced by `Name`. The resulting snapshot  will be
// committed and readonly. The `key` reference will no longer be available for
// lookup or removal. The returned string identifier for the committed snapshot
// is the same identifier of the original active snapshot. The provided context
// must contain a writable transaction.
// 实际上就是把处于KindActive的快照修改为KindCommitted状态，然后更新元数据信息
func CommitActive(ctx context.Context, key, name string, usage snapshots.Usage, opts ...snapshots.Opt) (string, error) {
	var (
		id   uint64
		base snapshots.Info
	)
	// 目前（v1.7.2）快照只能修改标签信息
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			return "", err
		}
	}

	// 获取/v1/snapshots以及/v1/parents桶，并传给func
	if err := withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		// 创建/v1/snapshots/<name>桶
		dbkt, err := bkt.CreateBucket([]byte(name))
		if err != nil {
			if err == bolt.ErrBucketExists {
				err = errdefs.ErrAlreadyExists
			}
			return fmt.Errorf("committed snapshot %v: %w", name, err)
		}
		// 获取/v1/snapshots/<key>桶
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return fmt.Errorf("failed to get active snapshot %q: %w", key, errdefs.ErrNotFound)
		}

		var si snapshots.Info
		// 从/v1/snapshots/<key>桶中读取快照信息
		if err := readSnapshot(sbkt, &id, &si); err != nil {
			return fmt.Errorf("failed to read active snapshot %q: %w", key, err)
		}

		// 只有KindActive状态的快照才可以提交
		if si.Kind != snapshots.KindActive {
			return fmt.Errorf("snapshot %q is not active: %w", key, errdefs.ErrFailedPrecondition)
		}
		si.Kind = snapshots.KindCommitted
		si.Created = time.Now().UTC()
		si.Updated = si.Created

		// Replace labels, do not inherit
		si.Labels = base.Labels

		// 修改快照到状态以及更新时间，保存到boltdb当中
		if err := putSnapshot(dbkt, id, si); err != nil {
			return err
		}
		// 修改快照使用的inodes大小以及磁盘大小到boltdb当中
		if err := putUsage(dbkt, usage); err != nil {
			return err
		}
		if err := bkt.DeleteBucket([]byte(key)); err != nil {
			return fmt.Errorf("failed to delete active snapshot %q: %w", key, err)
		}
		// 如果当前快照有parent，保存/v1/parents/<child-parent-composite>
		if si.Parent != "" {
			spbkt := bkt.Bucket([]byte(si.Parent))
			if spbkt == nil {
				return fmt.Errorf("missing parent %q of snapshot %q: %w", si.Parent, key, errdefs.ErrNotFound)
			}
			pid := readID(spbkt)

			// Updates parent back link to use new key
			if err := pbkt.Put(parentKey(pid, id), []byte(name)); err != nil {
				return fmt.Errorf("failed to update parent link %q from %q to %q: %w", pid, key, name, err)
			}
		}

		return nil
	}); err != nil {
		return "", err
	}

	return fmt.Sprintf("%d", id), nil
}

// IDMap returns all the IDs mapped to their key
// 读取所有快照的ID映射关系放入到map当中，map的key为快照ID，而value为快照的Key
func IDMap(ctx context.Context) (map[string]string, error) {
	m := map[string]string{}
	// 获取/v1/snapshots以及/v1/parents桶，并传给fn
	if err := withBucket(ctx, func(ctx context.Context, bkt, _ *bolt.Bucket) error {
		return bkt.ForEach(func(k, v []byte) error {
			// skip non buckets
			if v != nil {
				return nil
			}
			id := readID(bkt.Bucket(k))
			m[fmt.Sprintf("%d", id)] = string(k)
			return nil
		})
	}); err != nil {
		return nil, err
	}

	return m, nil
}

// 获取/v1/snapshots桶，从meta.db中观察，可以发现key = default/<index>/<digest>
func withSnapshotBucket(ctx context.Context, key string, fn func(context.Context, *bolt.Bucket, *bolt.Bucket) error) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok {
		return ErrNoTransaction
	}
	vbkt := tx.Bucket(bucketKeyStorageVersion)
	if vbkt == nil {
		return fmt.Errorf("bucket does not exist: %w", errdefs.ErrNotFound)
	}
	bkt := vbkt.Bucket(bucketKeySnapshot)
	if bkt == nil {
		return fmt.Errorf("snapshots bucket does not exist: %w", errdefs.ErrNotFound)
	}

	bkt = bkt.Bucket([]byte(key))
	if bkt == nil {
		return fmt.Errorf("snapshot does not exist: %w", errdefs.ErrNotFound)
	}

	return fn(ctx, bkt, vbkt.Bucket(bucketKeyParents))
}

// 获取/v1/snapshots以及/v1/parents桶，并传给fn
func withBucket(ctx context.Context, fn func(context.Context, *bolt.Bucket, *bolt.Bucket) error) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok {
		return ErrNoTransaction
	}
	bkt := tx.Bucket(bucketKeyStorageVersion)
	if bkt == nil {
		return fmt.Errorf("bucket does not exist: %w", errdefs.ErrNotFound)
	}
	return fn(ctx, bkt.Bucket(bucketKeySnapshot), bkt.Bucket(bucketKeyParents))
}

// 创建/v1/snapshots/parents桶
func createBucketIfNotExists(ctx context.Context, fn func(context.Context, *bolt.Bucket, *bolt.Bucket) error) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok {
		return ErrNoTransaction
	}

	bkt, err := tx.CreateBucketIfNotExists(bucketKeyStorageVersion)
	if err != nil {
		return fmt.Errorf("failed to create version bucket: %w", err)
	}
	sbkt, err := bkt.CreateBucketIfNotExists(bucketKeySnapshot)
	if err != nil {
		return fmt.Errorf("failed to create snapshots bucket: %w", err)
	}
	pbkt, err := bkt.CreateBucketIfNotExists(bucketKeyParents)
	if err != nil {
		return fmt.Errorf("failed to create parents bucket: %w", err)
	}
	return fn(ctx, sbkt, pbkt)
}

// 获取快照的所有parent的ID
func parents(bkt, pbkt *bolt.Bucket, parent uint64) (parents []string, err error) {
	for {
		parents = append(parents, fmt.Sprintf("%d", parent))

		parentKey := pbkt.Get(bucketKeyParent)
		if len(parentKey) == 0 {
			return
		}
		pbkt = bkt.Bucket(parentKey)
		if pbkt == nil {
			return nil, fmt.Errorf("missing parent: %w", errdefs.ErrNotFound)
		}

		// 读取parent的ID
		parent = readID(pbkt)
	}
}

func readKind(bkt *bolt.Bucket) (k snapshots.Kind) {
	kind := bkt.Get(bucketKeyKind)
	if len(kind) == 1 {
		k = snapshots.Kind(kind[0])
	}
	return
}

// 获取快照层的ID
func readID(bkt *bolt.Bucket) uint64 {
	id, _ := binary.Uvarint(bkt.Get(bucketKeyID))
	return id
}

// 直接从/v1/snapshot/<key>桶中读取kind, parent, createTime, updateTime, labels属性
func readSnapshot(bkt *bolt.Bucket, id *uint64, si *snapshots.Info) error {
	if id != nil {
		*id = readID(bkt)
	}
	if si != nil {
		si.Kind = readKind(bkt)
		si.Parent = string(bkt.Get(bucketKeyParent))

		if err := boltutil.ReadTimestamps(bkt, &si.Created, &si.Updated); err != nil {
			return err
		}

		labels, err := boltutil.ReadLabels(bkt)
		if err != nil {
			return err
		}
		si.Labels = labels
	}

	return nil
}

// 保存快照的ID, Kind, Parent, Label信息到/v1/snapshots/<key>桶中
func putSnapshot(bkt *bolt.Bucket, id uint64, si snapshots.Info) error {
	idEncoded, err := encodeID(id)
	if err != nil {
		return err
	}

	updates := [][2][]byte{
		{bucketKeyID, idEncoded},
		{bucketKeyKind, []byte{byte(si.Kind)}},
	}
	if si.Parent != "" {
		updates = append(updates, [2][]byte{bucketKeyParent, []byte(si.Parent)})
	}
	for _, v := range updates {
		if err := bkt.Put(v[0], v[1]); err != nil {
			return err
		}
	}
	if err := boltutil.WriteTimestamps(bkt, si.Created, si.Updated); err != nil {
		return err
	}
	return boltutil.WriteLabels(bkt, si.Labels)
}

func getUsage(bkt *bolt.Bucket, usage *snapshots.Usage) {
	usage.Inodes, _ = binary.Varint(bkt.Get(bucketKeyInodes))
	usage.Size, _ = binary.Varint(bkt.Get(bucketKeySize))
}

func putUsage(bkt *bolt.Bucket, usage snapshots.Usage) error {
	for _, v := range []struct {
		key   []byte
		value int64
	}{
		{bucketKeyInodes, usage.Inodes},
		{bucketKeySize, usage.Size},
	} {
		e, err := encodeSize(v.value)
		if err != nil {
			return err
		}
		if err := bkt.Put(v.key, e); err != nil {
			return err
		}
	}
	return nil
}

func encodeSize(size int64) ([]byte, error) {
	var (
		buf         [binary.MaxVarintLen64]byte
		sizeEncoded = buf[:]
	)
	sizeEncoded = sizeEncoded[:binary.PutVarint(sizeEncoded, size)]

	if len(sizeEncoded) == 0 {
		return nil, fmt.Errorf("failed encoding size = %v", size)
	}
	return sizeEncoded, nil
}

func encodeID(id uint64) ([]byte, error) {
	var (
		buf       [binary.MaxVarintLen64]byte
		idEncoded = buf[:]
	)
	idEncoded = idEncoded[:binary.PutUvarint(idEncoded, id)]

	if len(idEncoded) == 0 {
		return nil, fmt.Errorf("failed encoding id = %v", id)
	}
	return idEncoded, nil
}

func adaptSnapshot(info snapshots.Info) filters.Adaptor {
	return filters.AdapterFunc(func(fieldpath []string) (string, bool) {
		if len(fieldpath) == 0 {
			return "", false
		}

		switch fieldpath[0] {
		case "kind":
			switch info.Kind {
			case snapshots.KindActive:
				return "active", true
			case snapshots.KindView:
				return "view", true
			case snapshots.KindCommitted:
				return "committed", true
			}
		case "name":
			return info.Name, true
		case "parent":
			return info.Parent, true
		case "labels":
			if len(info.Labels) == 0 {
				return "", false
			}

			v, ok := info.Labels[strings.Join(fieldpath[1:], ".")]
			return v, ok
		}

		return "", false
	})
}
