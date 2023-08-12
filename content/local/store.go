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

package local

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/filters"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/pkg/randutil"
	"github.com/sirupsen/logrus"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

var bufPool = sync.Pool{
	New: func() interface{} {
		buffer := make([]byte, 1<<20)
		return &buffer
	},
}

// LabelStore is used to store mutable labels for digests
type LabelStore interface {
	// Get returns all the labels for the given digest
	Get(digest.Digest) (map[string]string, error)

	// Set sets all the labels for a given digest
	Set(digest.Digest, map[string]string) error

	// Update replaces the given labels for a digest,
	// a key with an empty value removes a label.
	Update(digest.Digest, map[string]string) (map[string]string, error)
}

// Store is digest-keyed store for content. All data written into the store is
// stored under a verifiable digest.
//
// Store can generally support multi-reader, single-writer ingest of data,
// including resumable ingest.
type store struct {
	root string
	ls   LabelStore
}

// NewStore returns a local content store
// TODO 似乎containerd目前并不支持给镜像层存储标签，都是使用的这个函数去初始化的
// content.local.store实现了对于blob和ingest的增删改查操作，这些操作都是基于文件的操作
func NewStore(root string) (content.Store, error) {
	return NewLabeledStore(root, nil)
}

// NewLabeledStore returns a new content store using the provided label store
//
// Note: content stores which are used underneath a metadata store may not
// require labels and should use `NewStore`. `NewLabeledStore` is primarily
// useful for tests or standalone implementations.
func NewLabeledStore(root string, ls LabelStore) (content.Store, error) {
	// 创建目录/var/lib/content/io.containerd.content.v1.content/ingest
	if err := os.MkdirAll(filepath.Join(root, "ingest"), 0777); err != nil {
		return nil, err
	}

	return &store{
		root: root,
		ls:   ls,
	}, nil
}

// Info Content服务实现Info非常简单，就是根据摘要信息拼接出这个摘要对应的镜像层的位置，然后当成一个普通文件读取其大小、创建时间、更新时间等
// Info接口用于根据摘要镜像层的信息，其实就是查看的二进制文件信息，在containerd中被称为blob
func (s *store) Info(ctx context.Context, dgst digest.Digest) (content.Info, error) {
	// blob为binary large object的缩写，也就是二进制形式的大对象
	// blob的概念可以参考这个连接：https://www.cloudflare.com/zh-cn/learning/cloud/what-is-blob-storage/
	// 这里实现的逻辑很简单，就是根据摘要信息拼接处此摘要指向的镜像层的路径，目录为：/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
	p, err := s.blobPath(dgst)
	if err != nil {
		return content.Info{}, fmt.Errorf("calculating blob info path: %w", err)
	}

	// 判断这个摘要对应的镜像层是否存在，毕竟在操作系统中，bolb就是一个普通文件而已，还是有可能被用户删除的
	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("content %v: %w", dgst, errdefs.ErrNotFound)
		}

		return content.Info{}, err
	}
	var labels map[string]string
	if s.ls != nil {
		labels, err = s.ls.Get(dgst)
		if err != nil {
			return content.Info{}, err
		}
	}
	// 直接读取操作系统中文件的大小、修改时间、创建时间等等
	return s.info(dgst, fi, labels), nil
}

func (s *store) info(dgst digest.Digest, fi os.FileInfo, labels map[string]string) content.Info {
	return content.Info{
		Digest:    dgst,
		Size:      fi.Size(),
		CreatedAt: fi.ModTime(),
		UpdatedAt: getATime(fi),
		Labels:    labels,
	}
}

// ReaderAt returns an io.ReaderAt for the blob.
// ReaderAt方法用于根据摘要读取镜像层的信息，其实就是读取blob文件（可以理解为镜像层就是一个二进制文件，在containerd中被称为blob）
func (s *store) ReaderAt(ctx context.Context, desc ocispec.Descriptor) (content.ReaderAt, error) {
	// 拼接出当前摘要所指向的镜像层的路径：/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
	p, err := s.blobPath(desc.Digest)
	if err != nil {
		return nil, fmt.Errorf("calculating blob path for ReaderAt: %w", err)
	}

	reader, err := OpenReader(p)
	if err != nil {
		return nil, fmt.Errorf("blob %s expected at %s: %w", desc.Digest, p, err)
	}

	return reader, nil
}

// Delete removes a blob by its digest.
//
// While this is safe to do concurrently, safe exist-removal logic must hold
// some global lock on the store.
// 根据摘要删除镜像层，镜像层其实就是一个二进制文件，在containerd中被称为blob
func (s *store) Delete(ctx context.Context, dgst digest.Digest) error {
	// 找到镜像层的存储路径：/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
	bp, err := s.blobPath(dgst)
	if err != nil {
		return fmt.Errorf("calculating blob path for delete: %w", err)
	}

	// 删除文件
	if err := os.RemoveAll(bp); err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		return fmt.Errorf("content %v: %w", dgst, errdefs.ErrNotFound)
	}

	return nil
}

// Update 用于更新镜像层的标签信息，TODO 看起来containerd并没有实现镜像层信息更新
// 根据摘要更新镜像层的信息，镜像层其实就是一个二进制文件，在containerd中被称为blob。
func (s *store) Update(ctx context.Context, info content.Info, fieldpaths ...string) (content.Info, error) {
	// 如果没有初始化标签存储器，肯定是不能更改的
	if s.ls == nil {
		return content.Info{}, fmt.Errorf("update not supported on immutable content store: %w", errdefs.ErrFailedPrecondition)
	}

	// 获取镜像层的存储路径：/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
	p, err := s.blobPath(info.Digest)
	if err != nil {
		return content.Info{}, fmt.Errorf("calculating blob path for update: %w", err)
	}

	// 判断镜像层是否存在
	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("content %v: %w", info.Digest, errdefs.ErrNotFound)
		}

		return content.Info{}, err
	}

	var (
		all    bool
		labels map[string]string
	)
	if len(fieldpaths) > 0 {
		for _, path := range fieldpaths {
			if strings.HasPrefix(path, "labels.") {
				if labels == nil {
					labels = map[string]string{}
				}

				key := strings.TrimPrefix(path, "labels.")
				labels[key] = info.Labels[key]
				continue
			}

			switch path {
			case "labels":
				all = true
				labels = info.Labels
			default:
				return content.Info{}, fmt.Errorf("cannot update %q field on content info %q: %w", path, info.Digest, errdefs.ErrInvalidArgument)
			}
		}
	} else {
		all = true
		labels = info.Labels
	}

	if all {
		err = s.ls.Set(info.Digest, labels)
	} else {
		labels, err = s.ls.Update(info.Digest, labels)
	}
	if err != nil {
		return content.Info{}, err
	}

	info = s.info(info.Digest, fi, labels)
	info.UpdatedAt = time.Now()

	if err := os.Chtimes(p, info.UpdatedAt, info.CreatedAt); err != nil {
		log.G(ctx).WithError(err).Warnf("could not change access time for %s", info.Digest)
	}

	return info, nil
}

// Walk 遍历containerd当前所有的镜像层，镜像层其实就是一个二进制文件，在containerd中被称为blob。
// 同时，如果制定了过滤器，那就按照指定的过滤器遍历符合条件的镜像层
func (s *store) Walk(ctx context.Context, fn content.WalkFunc, fs ...string) error {
	// 获取blob对象的存储路径：/var/lib/containerd/io.containerd.content.v1.content/blobs
	root := filepath.Join(s.root, "blobs")

	filter, err := filters.ParseAll(fs...)
	if err != nil {
		return err
	}

	var alg digest.Algorithm
	// 中规中矩的遍历目录
	return filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// 如果当前镜像层的算法不可用，直接退出
		if !fi.IsDir() && !alg.Available() {
			return nil
		}

		// TODO(stevvooe): There are few more cases with subdirs that should be
		// handled in case the layout gets corrupted. This isn't strict enough
		// and may spew bad data.

		// 忽略根目录
		if path == root {
			return nil
		}
		if filepath.Dir(path) == root {
			alg = digest.Algorithm(filepath.Base(path))

			if !alg.Available() {
				alg = ""
				return filepath.SkipDir
			}

			// descending into a hash directory
			return nil
		}

		dgst := digest.NewDigestFromEncoded(alg, filepath.Base(path))
		if err := dgst.Validate(); err != nil {
			// log error but don't report
			log.L.WithError(err).WithField("path", path).Error("invalid digest for blob path")
			// if we see this, it could mean some sort of corruption of the
			// store or extra paths not expected previously.
		}

		var labels map[string]string
		if s.ls != nil {
			labels, err = s.ls.Get(dgst)
			if err != nil {
				return err
			}
		}

		info := s.info(dgst, fi, labels)
		if !filter.Match(content.AdaptInfo(info)) {
			return nil
		}
		return fn(info)
	})
}

// Status 实际上就是通过镜像的信息
// 根据镜像名读取ingest信息
func (s *store) Status(ctx context.Context, ref string) (content.Status, error) {
	return s.status(s.ingestRoot(ref))
}

// ListStatuses 遍历containerd所包含的所有镜像的ingest信息
func (s *store) ListStatuses(ctx context.Context, fs ...string) ([]content.Status, error) {
	fp, err := os.Open(filepath.Join(s.root, "ingest"))
	if err != nil {
		return nil, err
	}

	defer fp.Close()

	fis, err := fp.Readdir(-1)
	if err != nil {
		return nil, err
	}

	filter, err := filters.ParseAll(fs...)
	if err != nil {
		return nil, err
	}

	var active []content.Status
	for _, fi := range fis {
		p := filepath.Join(s.root, "ingest", fi.Name())
		stat, err := s.status(p)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}

			// TODO(stevvooe): This is a common error if uploads are being
			// completed while making this listing. Need to consider taking a
			// lock on the whole store to coordinate this aspect.
			//
			// Another option is to cleanup downloads asynchronously and
			// coordinate this method with the cleanup process.
			//
			// For now, we just skip them, as they really don't exist.
			continue
		}

		if filter.Match(adaptStatus(stat)) {
			active = append(active, stat)
		}
	}

	return active, nil
}

// WalkStatusRefs is used to walk all status references
// Failed status reads will be logged and ignored, if
// this function is called while references are being altered,
// these error messages may be produced.
func (s *store) WalkStatusRefs(ctx context.Context, fn func(string) error) error {
	fp, err := os.Open(filepath.Join(s.root, "ingest"))
	if err != nil {
		return err
	}

	defer fp.Close()

	fis, err := fp.Readdir(-1)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		rf := filepath.Join(s.root, "ingest", fi.Name(), "ref")

		ref, err := readFileString(rf)
		if err != nil {
			log.G(ctx).WithError(err).WithField("path", rf).Error("failed to read ingest ref")
			continue
		}

		if err := fn(ref); err != nil {
			return err
		}
	}

	return nil
}

// status works like stat above except uses the path to the ingest.
func (s *store) status(ingestPath string) (content.Status, error) {
	dp := filepath.Join(ingestPath, "data")
	fi, err := os.Stat(dp)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("%s: %w", err.Error(), errdefs.ErrNotFound)
		}
		return content.Status{}, err
	}

	ref, err := readFileString(filepath.Join(ingestPath, "ref"))
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("%s: %w", err.Error(), errdefs.ErrNotFound)
		}
		return content.Status{}, err
	}

	startedAt, err := readFileTimestamp(filepath.Join(ingestPath, "startedat"))
	if err != nil {
		return content.Status{}, fmt.Errorf("could not read startedat: %w", err)
	}

	updatedAt, err := readFileTimestamp(filepath.Join(ingestPath, "updatedat"))
	if err != nil {
		return content.Status{}, fmt.Errorf("could not read updatedat: %w", err)
	}

	// because we don't write updatedat on every write, the mod time may
	// actually be more up to date.
	if fi.ModTime().After(updatedAt) {
		updatedAt = fi.ModTime()
	}

	return content.Status{
		Ref:       ref,
		Offset:    fi.Size(),
		Total:     s.total(ingestPath),
		UpdatedAt: updatedAt,
		StartedAt: startedAt,
	}, nil
}

func adaptStatus(status content.Status) filters.Adaptor {
	return filters.AdapterFunc(func(fieldpath []string) (string, bool) {
		if len(fieldpath) == 0 {
			return "", false
		}
		switch fieldpath[0] {
		case "ref":
			return status.Ref, true
		}

		return "", false
	})
}

// total attempts to resolve the total expected size for the write.
func (s *store) total(ingestPath string) int64 {
	totalS, err := readFileString(filepath.Join(ingestPath, "total"))
	if err != nil {
		return 0
	}

	total, err := strconv.ParseInt(totalS, 10, 64)
	if err != nil {
		// represents a corrupted file, should probably remove.
		return 0
	}

	return total
}

// Writer begins or resumes the active writer identified by ref. If the writer
// is already in use, an error is returned. Only one writer may be in use per
// ref at a time.
//
// The argument `ref` is used to uniquely identify a long-lived writer transaction.
// 用于生成ingest文件，下载镜像层时只需要传递镜像层的名字（ref），以及镜像层的描述符ocispec.Descriptor
func (s *store) Writer(ctx context.Context, opts ...content.WriterOpt) (content.Writer, error) {
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
	var lockErr error
	// 要想写入这个ingest文件，首先必须锁住这个文件，否则其他人可能会对这个文件进行读写
	for count := uint64(0); count < 10; count++ {
		if err := tryLock(wOpts.Ref); err != nil {
			if !errdefs.IsUnavailable(err) {
				return nil, err
			}

			lockErr = err
		} else {
			lockErr = nil
			break
		}
		time.Sleep(time.Millisecond * time.Duration(randutil.Intn(1<<count)))
	}

	if lockErr != nil {
		return nil, lockErr
	}

	w, err := s.writer(ctx, wOpts.Ref, wOpts.Desc.Size, wOpts.Desc.Digest)
	if err != nil {
		unlock(wOpts.Ref)
		return nil, err
	}

	return w, nil // lock is now held by w.
}

func (s *store) resumeStatus(ref string, total int64, digester digest.Digester) (content.Status, error) {
	// path = /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>
	// data = /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>data
	path, _, data := s.ingestPaths(ref)
	// 读取镜像下载状态
	status, err := s.status(path)
	if err != nil {
		return status, fmt.Errorf("failed reading status of resume write: %w", err)
	}
	if ref != status.Ref {
		// NOTE(stevvooe): This is fairly catastrophic. Either we have some
		// layout corruption or a hash collision for the ref key.
		return status, fmt.Errorf("ref key does not match: %v != %v", ref, status.Ref)
	}

	if total > 0 && status.Total > 0 && total != status.Total {
		return status, fmt.Errorf("provided total differs from status: %v != %v", total, status.Total)
	}

	//nolint:dupword
	// TODO(stevvooe): slow slow slow!!, send to goroutine or use resumable hashes
	fp, err := os.Open(data)
	if err != nil {
		return status, err
	}

	p := bufPool.Get().(*[]byte)
	status.Offset, err = io.CopyBuffer(digester.Hash(), fp, *p)
	bufPool.Put(p)
	fp.Close()
	return status, err
}

// writer provides the main implementation of the Writer method. The caller
// must hold the lock correctly and release on error if there is a problem.
// 1、ref为镜像名，total为镜像大小， expected为镜像数据计算出来的摘要信息。如果镜像下载完成，但是计算出来的摘要信息和希望的
// 摘要信息对不上，说明镜像下载的有问题
func (s *store) writer(ctx context.Context, ref string, total int64, expected digest.Digest) (content.Writer, error) {
	// TODO(stevvooe): Need to actually store expected here. We have
	// code in the service that shouldn't be dealing with this.
	// 如果当前镜像层已经下载，返回错误信息
	if expected != "" {
		p, err := s.blobPath(expected)
		if err != nil {
			return nil, fmt.Errorf("calculating expected blob path for writer: %w", err)
		}
		if _, err := os.Stat(p); err == nil {
			return nil, fmt.Errorf("content %v: %w", expected, errdefs.ErrAlreadyExists)
		}
	}

	// path = /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>
	// refp = /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>/ref
	// data = /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>data
	path, refp, data := s.ingestPaths(ref)

	var (
		// 摘要生成器
		digester  = digest.Canonical.Digester()
		offset    int64
		startedAt time.Time
		updatedAt time.Time
	)

	foundValidIngest := false
	// ensure that the ingest path has been created.
	// 创建/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>目录
	// 如果目录已经存在，那么需要判断这个目录是否是我们当前正在下载的镜像所对应的目录，如果是，那么继续下载。实际上这个功能就是断点续传
	if err := os.Mkdir(path, 0755); err != nil {
		if !os.IsExist(err) {
			return nil, err
		}
		// 恢复镜像下载点，进行断点续传
		status, err := s.resumeStatus(ref, total, digester)
		if err == nil {
			foundValidIngest = true
			updatedAt = status.UpdatedAt
			startedAt = status.StartedAt
			total = status.Total
			offset = status.Offset
		} else {
			logrus.Infof("failed to resume the status from path %s: %s. will recreate them", path, err.Error())
		}
	}

	// 如果当前镜像之前没有下载过，那么需要下载
	if !foundValidIngest {
		startedAt = time.Now()
		updatedAt = startedAt

		// the ingest is new, we need to setup the target location.
		// write the ref to a file for later use
		if err := os.WriteFile(refp, []byte(ref), 0666); err != nil {
			return nil, err
		}

		if err := writeTimestampFile(filepath.Join(path, "startedat"), startedAt); err != nil {
			return nil, err
		}

		if err := writeTimestampFile(filepath.Join(path, "updatedat"), startedAt); err != nil {
			return nil, err
		}

		if total > 0 {
			if err := os.WriteFile(filepath.Join(path, "total"), []byte(fmt.Sprint(total)), 0666); err != nil {
				return nil, err
			}
		}
	}

	fp, err := os.OpenFile(data, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	if _, err := fp.Seek(offset, io.SeekStart); err != nil {
		fp.Close()
		return nil, fmt.Errorf("could not seek to current write offset: %w", err)
	}

	return &writer{
		s:         s,
		fp:        fp,
		ref:       ref,
		path:      path,
		offset:    offset,
		total:     total,
		digester:  digester,
		startedAt: startedAt,
		updatedAt: updatedAt,
	}, nil
}

// Abort an active transaction keyed by ref. If the ingest is active, it will
// be cancelled. Any resources associated with the ingest will be cleaned.
// 移除镜像所指向的ingest的所有数据
func (s *store) Abort(ctx context.Context, ref string) error {
	// 获取镜像的ingest路径：/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>
	root := s.ingestRoot(ref)
	if err := os.RemoveAll(root); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("ingest ref %q: %w", ref, errdefs.ErrNotFound)
		}

		return err
	}

	return nil
}

func (s *store) blobPath(dgst digest.Digest) (string, error) {
	// 校验当前的摘要是否有效
	if err := dgst.Validate(); err != nil {
		return "", fmt.Errorf("cannot calculate blob path from invalid digest: %v: %w", err, errdefs.ErrInvalidArgument)
	}

	/*
		root@containerd:/var/lib/containerd/io.containerd.content.v1.content# tree blobs/
		blobs/
		└── sha256
		    ├── 00a1f6deb2b5d3294cb50e0a59dfc47f67650398d2f0151911e49a56bfd9c355
		    ├── 01085d60b3a624c06a7132ff0749efc6e6565d9f2531d7685ff559fb5d0f669f
		    ├── 029a81f05585f767fb7549af85a8f24479149e2a73710427a8775593fbe86159
		    ├── 05a79c7279f71f86a2a0d05eb72fcb56ea36139150f0a75cd87e80a4272e4e39
	*/
	// 拼接当前摘要所指向的镜像层的路径，路径为containerd的root配置指向的位置，拼接上blobs/sha256/<digest>
	// 默认root配置为：/var/lib/containerd，但是每个插件在初始化的时候会被修改root目录，对于content插件来说root目录为：/var/lib/containerd/io.containerd.content.v1.content
	// 因此路径为：/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
	return filepath.Join(s.root, "blobs", dgst.Algorithm().String(), dgst.Encoded()), nil
}

// 获取镜像的ingest信息，位置在：/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>
func (s *store) ingestRoot(ref string) string {
	// we take a digest of the ref to keep the ingest paths constant length.
	// Note that this is not the current or potential digest of incoming content.
	// TODO 这里的ref应该就是镜像信息，这个方法应该是通过镜像名获取镜像的摘要
	dgst := digest.FromString(ref)
	// 获取镜像的ingest路径：/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>
	return filepath.Join(s.root, "ingest", dgst.Encoded())
}

// ingestPaths are returned. The paths are the following:
//
// - root: entire ingest directory
// - ref: name of the starting ref, must be unique
// - data: file where data is written
// /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>/ref
// /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>data
func (s *store) ingestPaths(ref string) (string, string, string) {
	var (
		fp = s.ingestRoot(ref)
		rp = filepath.Join(fp, "ref")
		dp = filepath.Join(fp, "data")
	)

	return fp, rp, dp
}

func readFileString(path string) (string, error) {
	p, err := os.ReadFile(path)
	return string(p), err
}

// readFileTimestamp reads a file with just a timestamp present.
func readFileTimestamp(p string) (time.Time, error) {
	b, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("%s: %w", err.Error(), errdefs.ErrNotFound)
		}
		return time.Time{}, err
	}

	var t time.Time
	if err := t.UnmarshalText(b); err != nil {
		return time.Time{}, fmt.Errorf("could not parse timestamp file %v: %w", p, err)
	}

	return t, nil
}

func writeTimestampFile(p string, t time.Time) error {
	b, err := t.MarshalText()
	if err != nil {
		return err
	}
	return writeToCompletion(p, b, 0666)
}

func writeToCompletion(path string, data []byte, mode os.FileMode) error {
	tmp := fmt.Sprintf("%s.tmp", path)
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_SYNC, mode)
	if err != nil {
		return fmt.Errorf("create tmp file: %w", err)
	}
	_, err = f.Write(data)
	f.Close()
	if err != nil {
		return fmt.Errorf("write tmp file: %w", err)
	}
	err = os.Rename(tmp, path)
	if err != nil {
		return fmt.Errorf("rename tmp file: %w", err)
	}
	return nil
}
