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

package metadata

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/filters"
	"github.com/containerd/containerd/labels"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/metadata/boltutil"
	"github.com/containerd/containerd/namespaces"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	bolt "go.etcd.io/bbolt"
)

type contentStore struct {
	// Q:为什么contentStore又组合了content.Store接口，这玩意本来就是为了实现content.Store接口的嘛
	// A:因为我们在存储数据的时候不可能只是简简单单的存储元数据，真是的blob数据才是我们真正想要的保存的。因此这里在存储元数据的时候就需要依赖
	// 真正存储blob的底层实现，其实，看到后面我们会发现，这里其实就是content.local.store这个实现，用于保存blob到文件系统当中
	content.Store
	// 这里的DB可以认为就是boltdb，
	// TODO 为什么contentStore依赖DB，然而这个DB又依赖contentStore，这不是就是循环依赖了么？ 为什么需要这么设计？
	db *DB
	// TODO 共享什么？
	shared bool
	// TODO 这里保护的是那些资源的？
	l sync.RWMutex
}

// newContentStore returns a namespaced content store using an existing
// content store interface.
// policy defines the sharing behavior for content between namespaces. Both
// modes will result in shared storage in the backend for committed. Choose
// "shared" to prevent separate namespaces from having to pull the same content
// twice.  Choose "isolated" if the content must not be shared between
// namespaces.
//
// If the policy is "shared", writes will try to resolve the "expected" digest
// against the backend, allowing imports of content from other namespaces. In
// "isolated" mode, the client must prove they have the content by providing
// the entire blob before the content can be added to another namespace.
//
// Since we have only two policies right now, it's simpler using bool to
// represent it internally.
func newContentStore(db *DB, shared bool, cs content.Store) *contentStore {
	return &contentStore{
		Store:  cs,
		db:     db,
		shared: shared,
	}
}

// Info content.Info接口实现原理很简单，Info所需要的数据都是直接通过读取boltdb完成的
func (cs *contentStore) Info(ctx context.Context, dgst digest.Digest) (content.Info, error) {
	// 从当前请求当中获取名称空间
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return content.Info{}, err
	}

	var info content.Info
	// 开启只读事务
	if err := view(ctx, cs.db, func(tx *bolt.Tx) error {
		// 获取/v1/<namespace>/content/blob/<digest>桶
		bkt := getBlobBucket(tx, ns, dgst)
		if bkt == nil {
			return fmt.Errorf("content digest %v: %w", dgst, errdefs.ErrNotFound)
		}

		info.Digest = dgst
		// 读取/v1/<namespace>/content/blob/<digest>桶中的各个信息
		return readInfo(&info, bkt)
	}); err != nil {
		return content.Info{}, err
	}

	return info, nil
}

func (cs *contentStore) Update(ctx context.Context, info content.Info, fieldpaths ...string) (content.Info, error) {
	// 获取当前请求的名称空间
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return content.Info{}, err
	}

	cs.l.RLock()
	defer cs.l.RUnlock()

	updated := content.Info{
		Digest: info.Digest,
	}
	// 开启写事务
	if err := update(ctx, cs.db, func(tx *bolt.Tx) error {
		// 获取/v1/<namespace>/content/blob/<digest>桶
		bkt := getBlobBucket(tx, ns, info.Digest)
		if bkt == nil {
			return fmt.Errorf("content digest %v: %w", info.Digest, errdefs.ErrNotFound)
		}

		// 读取/v1/<namespace>/content/blob/<digest>桶中的所有数据到updated对象当中
		if err := readInfo(&updated, bkt); err != nil {
			return fmt.Errorf("info %q: %w", info.Digest, err)
		}

		// 通过路径表达式更新数据
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
					return fmt.Errorf("cannot update %q field on content info %q: %w", path, info.Digest, errdefs.ErrInvalidArgument)
				}
			}
		} else {
			// Set mutable fields
			updated.Labels = info.Labels
		}
		// 校验label信息
		if err := validateInfo(&updated); err != nil {
			return err
		}

		updated.UpdatedAt = time.Now().UTC()
		// 更新blob的信息
		return writeInfo(&updated, bkt)
	}); err != nil {
		return content.Info{}, err
	}
	return updated, nil
}

// Walk 遍历所有满足条件的Blob
func (cs *contentStore) Walk(ctx context.Context, fn content.WalkFunc, fs ...string) error {
	// 获取当前请求的名称空间
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}

	// 根据过滤表达式构造出过滤器
	filter, err := filters.ParseAll(fs...)
	if err != nil {
		return err
	}

	// TODO: Batch results to keep from reading all info into memory
	// 收集满足过滤条件的blob
	var infos []content.Info
	// 开启只读事务
	if err := view(ctx, cs.db, func(tx *bolt.Tx) error {
		// 获取/v1/<namespace>/content/blob桶
		bkt := getBlobsBucket(tx, ns)
		if bkt == nil {
			return nil
		}

		// 遍历/v1/<namespace>/content/blob桶，读取所有的blob信息，并过滤去满足条件的blob
		return bkt.ForEach(func(k, v []byte) error {
			// 解析摘要
			dgst, err := digest.Parse(string(k))
			if err != nil {
				// Not a digest, skip 有错误就直接跳过
				return nil
			}

			// 获取/v1/<namespace>/content/blob/<digest>通过
			bbkt := bkt.Bucket(k)
			if bbkt == nil {
				return nil
			}
			info := content.Info{
				Digest: dgst,
			}

			// 从桶中读取blob的信息
			if err := readInfo(&info, bkt.Bucket(k)); err != nil {
				return err
			}

			// 如果当前blob信息满足过滤器，那么把当前bolb信息保存起来
			if filter.Match(content.AdaptInfo(info)) {
				infos = append(infos, info)
			}
			return nil
		})
	}); err != nil {
		return err
	}

	for _, info := range infos {
		// 挨个执行用户传入的fn函数
		if err := fn(info); err != nil {
			return err
		}
	}

	return nil
}

// Delete 删除指定摘要的blob，实际上就是删除/v1/<namespace>/content/blob/<digest>桶，当然这个桶被删除了，那么这个桶地下所有的数据全部被删除
func (cs *contentStore) Delete(ctx context.Context, dgst digest.Digest) error {
	// 获取当前请求的名称空间
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}

	cs.l.RLock()
	defer cs.l.RUnlock()

	// 开启写事务
	return update(ctx, cs.db, func(tx *bolt.Tx) error {
		// 获取/v1/<namespace>/content/blob/<digest>桶
		bkt := getBlobBucket(tx, ns, dgst)
		if bkt == nil {
			return fmt.Errorf("content digest %v: %w", dgst, errdefs.ErrNotFound)
		}

		// 先获取获取/v1/<namespace>/content/blob桶通过，然后删除获取/v1/<namespace>/content/blob桶桶中
		// 的/v1/<namespace>/content/blob/<digest>桶
		if err := getBlobsBucket(tx, ns).DeleteBucket([]byte(dgst.String())); err != nil {
			return err
		}
		// 移除当前blob的lease信息  路径为/v1/<namespace>/leases/<lease-id>/content/<digest>
		if err := removeContentLease(ctx, tx, dgst); err != nil {
			return err
		}

		// Mark content store as dirty for triggering garbage collection
		atomic.AddUint32(&cs.db.dirty, 1)
		cs.db.dirtyCS = true

		return nil
	})
}

func (cs *contentStore) ListStatuses(ctx context.Context, fs ...string) ([]content.Status, error) {
	// 获取当前请求的名称空间
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}

	// 根据过滤表达式构造过滤器
	filter, err := filters.ParseAll(fs...)
	if err != nil {
		return nil, err
	}

	brefs := map[string]string{}
	// 开启读事务
	if err := view(ctx, cs.db, func(tx *bolt.Tx) error {
		// 获取/v1/<namespace>/content/ingests桶
		bkt := getIngestsBucket(tx, ns)
		if bkt == nil {
			return nil
		}

		// 遍历/v1/<namespace>/content/ingests桶
		return bkt.ForEach(func(k, v []byte) error {
			if v == nil {
				// TODO(dmcgowan): match name and potentially labels here
				brefs[string(k)] = string(bkt.Bucket(k).Get(bucketKeyRef))
			}
			return nil
		})
	}); err != nil {
		return nil, err
	}

	statuses := make([]content.Status, 0, len(brefs))
	for k, bref := range brefs {
		// 这里真正读取的是文件
		status, err := cs.Store.Status(ctx, bref)
		if err != nil {
			if errdefs.IsNotFound(err) {
				continue
			}
			return nil, err
		}
		status.Ref = k

		if filter.Match(adaptContentStatus(status)) {
			statuses = append(statuses, status)
		}
	}

	return statuses, nil

}

// 获取到的Ref一般长这样：default/1/layer-sha256:33486cc813b57bab328443c4145fb29da20d7e6a1dda857716e2be590445cbba
func getRef(tx *bolt.Tx, ns, ref string) string {
	// 获取/v1/<namespace>/content/ingests/<ref>桶
	bkt := getIngestBucket(tx, ns, ref)
	if bkt == nil {
		return ""
	}
	// 1、读取/v1/<namespace>/content/ingests/<ref>桶中ref的值
	// 2、这个值的内容一般为：default/1/layer-sha256:33486cc813b57bab328443c4145fb29da20d7e6a1dda857716e2be590445cbba
	v := bkt.Get(bucketKeyRef)
	if len(v) == 0 {
		return ""
	}
	return string(v)
}

// Status 实现思路很简单，先查Boltdb这个KV数据库，如果没有找到元数据，就直接返回错误；如果找到了，就通过local.store读取
// /var/lib/contaienrd/io.containerd.content.v1.content/ingest目录中的文件数据
func (cs *contentStore) Status(ctx context.Context, ref string) (content.Status, error) {
	// 获取当前请求的名称空间
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return content.Status{}, err
	}

	var bref string
	// 开启读事务
	if err := view(ctx, cs.db, func(tx *bolt.Tx) error {
		// 1、读取/v1/<namespace>/content/ingests/<ref>桶中的内容
		// 2、获取到的Ref一般长这样：default/1/layer-sha256:33486cc813b57bab328443c4145fb29da20d7e6a1dda857716e2be590445cbba
		bref = getRef(tx, ns, ref)
		if bref == "" {
			return fmt.Errorf("reference %v: %w", ref, errdefs.ErrNotFound)
		}

		return nil
	}); err != nil {
		return content.Status{}, err
	}

	// 通过Store服务读取真是的文件系统中的ingest的值
	st, err := cs.Store.Status(ctx, bref)
	if err != nil {
		return content.Status{}, err
	}
	st.Ref = ref
	return st, nil
}

func (cs *contentStore) Abort(ctx context.Context, ref string) error {
	// 获取当前请求的名称空间
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}

	cs.l.RLock()
	defer cs.l.RUnlock()

	// 开启读事务
	return update(ctx, cs.db, func(tx *bolt.Tx) error {
		// 获取/v1/<namespace>/content/ingests桶
		ibkt := getIngestsBucket(tx, ns)
		if ibkt == nil {
			return fmt.Errorf("reference %v: %w", ref, errdefs.ErrNotFound)
		}
		// 获取/v1/<namespace>/content/ingests/<ref>桶
		bkt := ibkt.Bucket([]byte(ref))
		if bkt == nil {
			return fmt.Errorf("reference %v: %w", ref, errdefs.ErrNotFound)
		}
		// 读取/v1/<namespace>/content/ingests/<ref>桶中key为ref的值
		bref := string(bkt.Get(bucketKeyRef))
		if bref == "" {
			return fmt.Errorf("reference %v: %w", ref, errdefs.ErrNotFound)
		}
		// 读取/v1/<namespace>/content/ingests/<ref>桶中key为expected的值
		expected := string(bkt.Get(bucketKeyExpected))
		// 删除桶
		if err := ibkt.DeleteBucket([]byte(ref)); err != nil {
			return err
		}

		if err := removeIngestLease(ctx, tx, ref); err != nil {
			return err
		}

		// if not shared content, delete active ingest on backend
		if expected == "" {
			return cs.Store.Abort(ctx, bref)
		}

		return nil
	})

}

func (cs *contentStore) Writer(ctx context.Context, opts ...content.WriterOpt) (content.Writer, error) {
	var wOpts content.WriterOpts
	for _, opt := range opts {
		if err := opt(&wOpts); err != nil {
			return nil, err
		}
	}
	// TODO(AkihiroSuda): we could create a random string or one calculated based on the context
	// https://github.com/containerd/containerd/issues/2129#issuecomment-380255019
	if wOpts.Ref == "" {
		return nil, fmt.Errorf("ref must not be empty: %w", errdefs.ErrInvalidArgument)
	}
	// 获取当前请求的名称空间
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}

	cs.l.RLock()
	defer cs.l.RUnlock()

	var (
		w      content.Writer
		exists bool
		bref   string
	)
	// 开启写事务
	if err := update(ctx, cs.db, func(tx *bolt.Tx) error {
		var shared bool
		if wOpts.Desc.Digest != "" {
			// 获取/v1/<namespace>/content/blob/<digest>桶
			cbkt := getBlobBucket(tx, ns, wOpts.Desc.Digest)
			if cbkt != nil {
				// Add content to lease to prevent other reference removals
				// from effecting this object during a provided lease
				// 在/v1/<namespace>/leases/<id>/content桶写入摘要信息
				// TODO 这里写入Lease的意义何在？为了保障什么？
				if err := addContentLease(ctx, tx, wOpts.Desc.Digest); err != nil {
					return fmt.Errorf("unable to lease content: %w", err)
				}
				// Return error outside of transaction to ensure
				// commit succeeds with the lease.
				exists = true
				return nil
			}

			if cs.shared || isSharedContent(tx, wOpts.Desc.Digest) {
				if st, err := cs.Store.Info(ctx, wOpts.Desc.Digest); err == nil {
					// Ensure the expected size is the same, it is likely
					// an error if the size is mismatched but the caller
					// must resolve this on commit
					if wOpts.Desc.Size == 0 || wOpts.Desc.Size == st.Size {
						shared = true
						wOpts.Desc.Size = st.Size
					}
				}
			}
		}

		bkt, err := createIngestBucket(tx, ns, wOpts.Ref)
		if err != nil {
			return err
		}

		leased, err := addIngestLease(ctx, tx, wOpts.Ref)
		if err != nil {
			return err
		}

		brefb := bkt.Get(bucketKeyRef)
		if brefb == nil {
			sid, err := bkt.NextSequence()
			if err != nil {
				return err
			}

			bref = createKey(sid, ns, wOpts.Ref)
			if err := bkt.Put(bucketKeyRef, []byte(bref)); err != nil {
				return err
			}
		} else {
			bref = string(brefb)
		}
		if !leased {
			// Add timestamp to allow aborting once stale
			// When lease is set the ingest should be aborted
			// after lease it belonged to is deleted.
			// Expiration can be configurable in the future to
			// give more control to the daemon, however leases
			// already give users more control of expiration.
			expireAt := time.Now().UTC().Add(24 * time.Hour)
			if err := writeExpireAt(expireAt, bkt); err != nil {
				return err
			}
		}

		if shared {
			if err := bkt.Put(bucketKeyExpected, []byte(wOpts.Desc.Digest)); err != nil {
				return err
			}
		} else {
			// Do not use the passed in expected value here since it was
			// already checked against the user metadata. The content must
			// be committed in the namespace before it will be seen as
			// available in the current namespace.
			desc := wOpts.Desc
			desc.Digest = ""
			// 可以看到这里还是通过Store真正的下载数据
			w, err = cs.Store.Writer(ctx, content.WithRef(bref), content.WithDescriptor(desc))
		}
		return err
	}); err != nil {
		return nil, err
	}
	if exists {
		return nil, fmt.Errorf("content %v: %w", wOpts.Desc.Digest, errdefs.ErrAlreadyExists)
	}

	return &namespacedWriter{
		ctx:       ctx,
		ref:       wOpts.Ref,
		namespace: ns,
		db:        cs.db,
		provider:  cs.Store,
		l:         &cs.l,
		w:         w,
		bref:      bref,
		started:   time.Now(),
		desc:      wOpts.Desc,
	}, nil
}

type namespacedWriter struct {
	ctx       context.Context
	ref       string
	namespace string
	db        transactor
	provider  interface {
		content.Provider
		content.Ingester
	}
	l *sync.RWMutex

	w content.Writer

	bref    string
	started time.Time
	desc    ocispec.Descriptor
}

func (nw *namespacedWriter) Close() error {
	if nw.w != nil {
		return nw.w.Close()
	}
	return nil
}

func (nw *namespacedWriter) Write(p []byte) (int, error) {
	// if no writer, first copy and unshare before performing write
	if nw.w == nil {
		if len(p) == 0 {
			return 0, nil
		}

		if err := nw.createAndCopy(nw.ctx, nw.desc); err != nil {
			return 0, err
		}
	}

	return nw.w.Write(p)
}

func (nw *namespacedWriter) Digest() digest.Digest {
	if nw.w != nil {
		return nw.w.Digest()
	}
	return nw.desc.Digest
}

func (nw *namespacedWriter) Truncate(size int64) error {
	if nw.w != nil {
		return nw.w.Truncate(size)
	}
	desc := nw.desc
	desc.Size = size
	desc.Digest = ""
	return nw.createAndCopy(nw.ctx, desc)
}

func (nw *namespacedWriter) createAndCopy(ctx context.Context, desc ocispec.Descriptor) error {
	nwDescWithoutDigest := desc
	nwDescWithoutDigest.Digest = ""
	w, err := nw.provider.Writer(ctx, content.WithRef(nw.bref), content.WithDescriptor(nwDescWithoutDigest))
	if err != nil {
		return err
	}

	if desc.Size > 0 {
		ra, err := nw.provider.ReaderAt(ctx, nw.desc)
		if err != nil {
			w.Close()
			return err
		}
		defer ra.Close()

		if err := content.CopyReaderAt(w, ra, desc.Size); err != nil {
			w.Close()
			return err
		}
	}
	nw.w = w

	return nil
}

func (nw *namespacedWriter) Commit(ctx context.Context, size int64, expected digest.Digest, opts ...content.Opt) error {
	ctx = namespaces.WithNamespace(ctx, nw.namespace)

	nw.l.RLock()
	defer nw.l.RUnlock()

	var innerErr error

	if err := update(ctx, nw.db, func(tx *bolt.Tx) error {
		dgst, err := nw.commit(ctx, tx, size, expected, opts...)
		if err != nil {
			if !errdefs.IsAlreadyExists(err) {
				return err
			}
			innerErr = err
		}
		bkt := getIngestsBucket(tx, nw.namespace)
		if bkt != nil {
			if err := bkt.DeleteBucket([]byte(nw.ref)); err != nil && err != bolt.ErrBucketNotFound {
				return err
			}
		}
		if err := removeIngestLease(ctx, tx, nw.ref); err != nil {
			return err
		}
		return addContentLease(ctx, tx, dgst)
	}); err != nil {
		return err
	}

	return innerErr
}

func (nw *namespacedWriter) commit(ctx context.Context, tx *bolt.Tx, size int64, expected digest.Digest, opts ...content.Opt) (digest.Digest, error) {
	var base content.Info
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			if nw.w != nil {
				nw.w.Close()
			}
			return "", err
		}
	}
	if err := validateInfo(&base); err != nil {
		if nw.w != nil {
			nw.w.Close()
		}
		return "", err
	}

	var actual digest.Digest
	if nw.w == nil {
		if size != 0 && size != nw.desc.Size {
			return "", fmt.Errorf("%q failed size validation: %v != %v: %w", nw.ref, nw.desc.Size, size, errdefs.ErrFailedPrecondition)
		}
		if expected != "" && expected != nw.desc.Digest {
			return "", fmt.Errorf("%q unexpected digest: %w", nw.ref, errdefs.ErrFailedPrecondition)
		}
		size = nw.desc.Size
		actual = nw.desc.Digest
	} else {
		status, err := nw.w.Status()
		if err != nil {
			nw.w.Close()
			return "", err
		}
		if size != 0 && size != status.Offset {
			nw.w.Close()
			return "", fmt.Errorf("%q failed size validation: %v != %v: %w", nw.ref, status.Offset, size, errdefs.ErrFailedPrecondition)
		}
		size = status.Offset

		if err := nw.w.Commit(ctx, size, expected); err != nil && !errdefs.IsAlreadyExists(err) {
			return "", err
		}
		actual = nw.w.Digest()
	}

	bkt, err := createBlobBucket(tx, nw.namespace, actual)
	if err != nil {
		if err == bolt.ErrBucketExists {
			return actual, fmt.Errorf("content %v: %w", actual, errdefs.ErrAlreadyExists)
		}
		return "", err
	}

	commitTime := time.Now().UTC()

	sizeEncoded, err := encodeInt(size)
	if err != nil {
		return "", err
	}

	if err := boltutil.WriteTimestamps(bkt, commitTime, commitTime); err != nil {
		return "", err
	}
	if err := boltutil.WriteLabels(bkt, base.Labels); err != nil {
		return "", err
	}
	return actual, bkt.Put(bucketKeySize, sizeEncoded)
}

func (nw *namespacedWriter) Status() (st content.Status, err error) {
	if nw.w != nil {
		st, err = nw.w.Status()
	} else {
		st.Offset = nw.desc.Size
		st.Total = nw.desc.Size
		st.StartedAt = nw.started
		st.UpdatedAt = nw.started
		st.Expected = nw.desc.Digest
	}
	if err == nil {
		st.Ref = nw.ref
	}
	return
}

func (cs *contentStore) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	// 判断当前摘要对应的桶是否能够正常读取
	if err := cs.checkAccess(ctx, desc.Digest); err != nil {
		return nil, err
	}

	// 由于是要读取真是的blob数据，因此需要通过Store对象来进行读取
	return cs.Store.ReaderAt(ctx, desc)
}

func (cs *contentStore) checkAccess(ctx context.Context, dgst digest.Digest) error {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}

	return view(ctx, cs.db, func(tx *bolt.Tx) error {
		// 获取/v1/<namespace>/content/blob/<digest>桶
		bkt := getBlobBucket(tx, ns, dgst)
		if bkt == nil {
			return fmt.Errorf("content digest %v: %w", dgst, errdefs.ErrNotFound)
		}
		return nil
	})
}

func isSharedContent(tx *bolt.Tx, dgst digest.Digest) bool {
	v1bkt := tx.Bucket(bucketKeyVersion)
	if v1bkt == nil {
		return false
	}
	// iterate through each namespace
	v1c := v1bkt.Cursor()
	for nk, _ := v1c.First(); nk != nil; nk, _ = v1c.Next() {
		ns := string(nk)
		lbkt := getNamespaceLabelsBucket(tx, ns)
		if lbkt == nil {
			continue
		}
		// iterate through each label
		lbc := lbkt.Cursor()
		for k, v := lbc.First(); k != nil; k, v = lbc.Next() {
			if string(k) == labels.LabelSharedNamespace {
				if string(v) == "true" && getBlobBucket(tx, ns, dgst) != nil {
					return true
				}
				break
			}
		}
	}
	return false
}

func validateInfo(info *content.Info) error {
	for k, v := range info.Labels {
		if err := labels.Validate(k, v); err != nil {
			return fmt.Errorf("info.Labels: %w", err)
		}
	}

	return nil
}

// 所谓的读取信息，其实就是直接读取的boltdb的信息
// bkt的桶路径为：/v1/<namespace>/content/blob/<digest>
func readInfo(info *content.Info, bkt *bolt.Bucket) error {
	// 从/v1/<namespace>/content/blob/<digest>/createdat桶中获取创建时间赋值给info.CreatedAt
	// 从/v1/<namespace>/content/blob/<digest>/updatedat桶中获取更新时间赋值给info.UpdatedAt
	if err := boltutil.ReadTimestamps(bkt, &info.CreatedAt, &info.UpdatedAt); err != nil {
		return err
	}

	// 获取/v1/<namespace>/content/blob/<digest>/labels桶中的所有KV键值对最为label
	labels, err := boltutil.ReadLabels(bkt)
	if err != nil {
		return err
	}
	info.Labels = labels

	// 获取/v1/<namespace>/content/blob/<digest>桶中当前摘要指向的镜像层的大小
	if v := bkt.Get(bucketKeySize); len(v) > 0 {
		// 看到没有，这里还是直接忽略了这个错误，因为在开发者看来，这里是不可能会转换失败的，因为元数据的写入都是由自己控制的
		info.Size, _ = binary.Varint(v)
	}

	return nil
}

// 写入blob信息
func writeInfo(info *content.Info, bkt *bolt.Bucket) error {
	if err := boltutil.WriteTimestamps(bkt, info.CreatedAt, info.UpdatedAt); err != nil {
		return err
	}

	if err := boltutil.WriteLabels(bkt, info.Labels); err != nil {
		return fmt.Errorf("writing labels for info %v: %w", info.Digest, err)
	}

	// Write size
	sizeEncoded, err := encodeInt(info.Size)
	if err != nil {
		return err
	}

	return bkt.Put(bucketKeySize, sizeEncoded)
}

func readExpireAt(bkt *bolt.Bucket) (*time.Time, error) {
	v := bkt.Get(bucketKeyExpireAt)
	if v == nil {
		return nil, nil
	}
	t := &time.Time{}
	if err := t.UnmarshalBinary(v); err != nil {
		return nil, err
	}
	return t, nil
}

func writeExpireAt(expire time.Time, bkt *bolt.Bucket) error {
	expireAt, err := expire.MarshalBinary()
	if err != nil {
		return err
	}
	return bkt.Put(bucketKeyExpireAt, expireAt)
}

// garbageCollect removes all contents that are no longer used.
func (cs *contentStore) garbageCollect(ctx context.Context) (d time.Duration, err error) {
	cs.l.Lock()
	t1 := time.Now()
	defer func() {
		if err == nil {
			d = time.Since(t1)
		}
		cs.l.Unlock()
	}()

	contentSeen := map[string]struct{}{}
	ingestSeen := map[string]struct{}{}
	if err := cs.db.View(func(tx *bolt.Tx) error {
		v1bkt := tx.Bucket(bucketKeyVersion)
		if v1bkt == nil {
			return nil
		}

		// iterate through each namespace
		v1c := v1bkt.Cursor()

		for k, v := v1c.First(); k != nil; k, v = v1c.Next() {
			if v != nil {
				continue
			}

			cbkt := v1bkt.Bucket(k).Bucket(bucketKeyObjectContent)
			if cbkt == nil {
				continue
			}
			bbkt := cbkt.Bucket(bucketKeyObjectBlob)
			if bbkt != nil {
				if err := bbkt.ForEach(func(ck, cv []byte) error {
					if cv == nil {
						contentSeen[string(ck)] = struct{}{}
					}
					return nil
				}); err != nil {
					return err
				}
			}

			ibkt := cbkt.Bucket(bucketKeyObjectIngests)
			if ibkt != nil {
				if err := ibkt.ForEach(func(ref, v []byte) error {
					if v == nil {
						bkt := ibkt.Bucket(ref)
						// expected here may be from a different namespace
						// so much be explicitly retained from the ingest
						// in case it was removed from the other namespace
						expected := bkt.Get(bucketKeyExpected)
						if len(expected) > 0 {
							contentSeen[string(expected)] = struct{}{}
						}
						bref := bkt.Get(bucketKeyRef)
						if len(bref) > 0 {
							ingestSeen[string(bref)] = struct{}{}
						}
					}
					return nil
				}); err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil {
		return 0, err
	}

	err = cs.Store.Walk(ctx, func(info content.Info) error {
		if _, ok := contentSeen[info.Digest.String()]; !ok {
			if err := cs.Store.Delete(ctx, info.Digest); err != nil {
				return err
			}
			log.G(ctx).WithField("digest", info.Digest).Debug("removed content")
		}
		return nil
	})
	if err != nil {
		return
	}

	// If the content store has implemented a more efficient walk function
	// then use that else fallback to reading all statuses which may
	// cause reading of unneeded metadata.
	type statusWalker interface {
		WalkStatusRefs(context.Context, func(string) error) error
	}
	if w, ok := cs.Store.(statusWalker); ok {
		err = w.WalkStatusRefs(ctx, func(ref string) error {
			if _, ok := ingestSeen[ref]; !ok {
				if err := cs.Store.Abort(ctx, ref); err != nil {
					return err
				}
				log.G(ctx).WithField("ref", ref).Debug("cleanup aborting ingest")
			}
			return nil
		})
	} else {
		var statuses []content.Status
		statuses, err = cs.Store.ListStatuses(ctx)
		if err != nil {
			return 0, err
		}
		for _, status := range statuses {
			if _, ok := ingestSeen[status.Ref]; !ok {
				if err = cs.Store.Abort(ctx, status.Ref); err != nil {
					return
				}
				log.G(ctx).WithField("ref", status.Ref).Debug("cleanup aborting ingest")
			}
		}
	}
	return
}
