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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/opencontainers/go-digest"
)

// writer represents a write transaction against the blob store.
type writer struct {
	s         *store          // local.store，用于实现文件的增删改查
	fp        *os.File        // opened data file
	path      string          // path to writer dir  /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>
	ref       string          // ref key /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>/ref
	offset    int64           // 偏移量，当偏移量等于total，说明镜像层下载完成
	total     int64           // 当前要下载的镜像层的总大小
	digester  digest.Digester // 摘要生成器
	startedAt time.Time
	updatedAt time.Time
}

func (w *writer) Status() (content.Status, error) {
	return content.Status{
		Ref:       w.ref,
		Offset:    w.offset,
		Total:     w.total,
		StartedAt: w.startedAt,
		UpdatedAt: w.updatedAt,
	}, nil
}

// Digest returns the current digest of the content, up to the current write.
//
// Cannot be called concurrently with `Write`.
func (w *writer) Digest() digest.Digest {
	return w.digester.Digest()
}

// Write p to the transaction.
//
// Note that writes are unbuffered to the backing file. When writing, it is
// recommended to wrap in a bufio.Writer or, preferably, use io.CopyBuffer.
// 向镜像层中写入数据，同时更新镜像层的哈希
func (w *writer) Write(p []byte) (n int, err error) {
	n, err = w.fp.Write(p)
	w.digester.Hash().Write(p[:n])
	w.offset += int64(len(p))
	w.updatedAt = time.Now()
	return n, err
}

// Commit 执行此动作时，表明镜像已经下载完成，主要是把/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>目录中的数据拷贝到
// /var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>当中
func (w *writer) Commit(ctx context.Context, size int64, expected digest.Digest, opts ...content.Opt) error {
	// Ensure even on error the writer is fully closed
	// 当镜像下载完成之后，自然需要解锁
	defer unlock(w.ref)

	var base content.Info
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			return err
		}
	}

	fp := w.fp
	w.fp = nil

	if fp == nil {
		return fmt.Errorf("cannot commit on closed writer: %w", errdefs.ErrFailedPrecondition)
	}

	// 刷新内存中的数据到磁盘上
	if err := fp.Sync(); err != nil {
		fp.Close()
		return fmt.Errorf("sync failed: %w", err)
	}

	fi, err := fp.Stat()
	closeErr := fp.Close()
	if err != nil {
		return fmt.Errorf("stat on ingest file failed: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to close ingest file: %w", closeErr)
	}

	if size > 0 && size != fi.Size() {
		return fmt.Errorf("unexpected commit size %d, expected %d: %w", fi.Size(), size, errdefs.ErrFailedPrecondition)
	}

	// 计算镜像层的摘要，如果摘要计算出来的和希望摘要不等，说明镜像下载的有问题
	dgst := w.digester.Digest()
	if expected != "" && expected != dgst {
		return fmt.Errorf("unexpected commit digest %s, expected %s: %w", dgst, expected, errdefs.ErrFailedPrecondition)
	}

	var (
		// /var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>/data
		ingest = filepath.Join(w.path, "data")
		// /var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
		target, _ = w.s.blobPath(dgst) // ignore error because we calculated this dgst
	)

	// make sure parent directories of blob exist
	// 创建目录，准备把下载完成的ingest镜像数据拷贝到blob当中
	if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
		return err
	}

	if _, err := os.Stat(target); err == nil {
		// collision with the target file!
		// 清空目录
		if err := os.RemoveAll(w.path); err != nil {
			log.G(ctx).WithField("ref", w.ref).WithField("path", w.path).Error("failed to remove ingest directory")
		}
		return fmt.Errorf("content %v: %w", dgst, errdefs.ErrAlreadyExists)
	}

	// 重命名，其实就是拷贝数据
	if err := os.Rename(ingest, target); err != nil {
		return err
	}

	// Ingest has now been made available in the content store, attempt to complete
	// setting metadata but errors should only be logged and not returned since
	// the content store cannot be cleanly rolled back.

	commitTime := time.Now()
	if err := os.Chtimes(target, commitTime, commitTime); err != nil {
		log.G(ctx).WithField("digest", dgst).Error("failed to change file time to commit time")
	}

	// clean up!!
	// 清理/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>目录中的所有数据
	if err := os.RemoveAll(w.path); err != nil {
		log.G(ctx).WithField("ref", w.ref).WithField("path", w.path).Error("failed to remove ingest directory")
	}

	// 设置标签
	if w.s.ls != nil && base.Labels != nil {
		if err := w.s.ls.Set(dgst, base.Labels); err != nil {
			log.G(ctx).WithField("digest", dgst).Error("failed to set labels")
		}
	}

	// change to readonly, more important for read, but provides _some_
	// protection from this point on. We use the existing perms with a mask
	// only allowing reads honoring the umask on creation.
	//
	// This removes write and exec, only allowing read per the creation umask.
	//
	// NOTE: Windows does not support this operation
	if runtime.GOOS != "windows" {
		// 修改权限
		if err := os.Chmod(target, (fi.Mode()&os.ModePerm)&^0333); err != nil {
			log.G(ctx).WithField("ref", w.ref).Error("failed to make readonly")
		}
	}

	return nil
}

// Close the writer, flushing any unwritten data and leaving the progress in
// tact.
//
// If one needs to resume the transaction, a new writer can be obtained from
// `Ingester.Writer` using the same key. The write can then be continued
// from it was left off.
//
// To abandon a transaction completely, first call close then `IngestManager.Abort` to
// clean up the associated resources.
func (w *writer) Close() (err error) {
	if w.fp != nil {
		w.fp.Sync()
		err = w.fp.Close()
		writeTimestampFile(filepath.Join(w.path, "updatedat"), w.updatedAt)
		w.fp = nil
		unlock(w.ref)
		return
	}

	return nil
}

func (w *writer) Truncate(size int64) error {
	if size != 0 {
		return errors.New("Truncate: unsupported size")
	}
	w.offset = 0
	w.digester.Hash().Reset()
	if _, err := w.fp.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.fp.Truncate(0)
}
