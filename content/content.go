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

package content

import (
	"context"
	"io"
	"time"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Store combines the methods of content-oriented interfaces into a set that
// are commonly provided by complete implementations.
//
// Overall content lifecycle:
//   - Ingester is used to initiate a write operation (aka ingestion)
//   - IngestManager is used to manage (e.g. list, abort) active ingestions
//   - Once an ingestion is complete (see Writer.Commit), Provider is used to
//     query a single piece of content by its digest
//   - Manager is used to manage (e.g. list, delete) previously committed content
//
// Note that until ingestion is complete, its content is not visible through
// Provider or Manager. Once ingestion is complete, it is no longer exposed
// through IngestManager.
// Q: blob和ingest的区别是啥？
// A: blob指的是已经完整下载的镜像层，而ingest则指的是没有完全下载的镜像层，可以用于做断点续传
type Store interface {
	Manager       // Manager实际上就是对于镜像层获取信息、修改信息、遍历镜像层以及删除镜像层的封装，blob的读取接口
	Provider      // 用于读取镜像层，blob的读取接口  为什么没有blob的写入接口，因为ingest其实就是blob，一旦ingest写入完成，就会被转为blob
	IngestManager // 这个接口实际上是对于ingest的管理，主要是获取ingest信息以及删除, ingest的读取接口
	Ingester      // ingest的写入接口
}

// ReaderAt extends the standard io.ReaderAt interface with reporting of Size and io.Closer
// 这里使用 io.ReaderAt 来读取数据，也就是说用户可以指定偏移。 TODO 这种读取方式是否在位端点续传做准备？
type ReaderAt interface {
	io.ReaderAt
	io.Closer
	Size() int64
}

// Provider provides a reader interface for specific content
// 此接口可以用于读取某镜像层（通过摘要）数据，并且可以指定偏移量
// 读取路径为：/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
type Provider interface {
	// ReaderAt only requires desc.Digest to be set.
	// Other fields in the descriptor may be used internally for resolving
	// the location of the actual data.
	ReaderAt(ctx context.Context, desc ocispec.Descriptor) (ReaderAt, error)
}

// Ingester writes content
// ingest的写入接口
type Ingester interface {
	// Writer initiates a writing operation (aka ingestion). A single ingestion
	// is uniquely identified by its ref, provided using a WithRef option.
	// Writer can be called multiple times with the same ref to access the same
	// ingestion.
	// Once all the data is written, use Writer.Commit to complete the ingestion.
	Writer(ctx context.Context, opts ...WriterOpt) (Writer, error)
}

// IngestManager provides methods for managing ingestions. An ingestion is a
// not-yet-complete writing operation initiated using Ingester and identified
// by a ref string.
// 1、到底如何理解ingest这个概念？ 根据注释的含义，实际上就是ingest就是一个还未完成的写操作，这里的写操作肯定是指的镜像的写操作
// IngestManager用于抽象还未完成镜像层的查询、删除操作
// 2、ref其实就是blob的摘要，类似：c634a1bed226111dcfe0dc8b55e5ce067ba284eff8afd4bcf3b3ef218dbc5f09
type IngestManager interface {
	// Status returns the status of the provided ref.
	// 根据ingest的摘要信息获取当前ingest的下载情况，可以用来断点续传
	Status(ctx context.Context, ref string) (Status, error)

	// ListStatuses returns the status of any active ingestions whose ref match
	// the provided regular expression. If empty, all active ingestions will be
	// returned.
	// 返回所有ingest的信息，并根据过滤器过滤不需要的ingest
	ListStatuses(ctx context.Context, filters ...string) ([]Status, error)

	// Abort completely cancels the ingest operation targeted by ref.
	// 移除ingest的所有数据
	Abort(ctx context.Context, ref string) error
}

// Info holds content specific information
//
// TODO(stevvooe): Consider a very different name for this struct. Info is way
// to general. It also reads very weird in certain context, like pluralization.
// 1、镜像层的Info信息只需要获取摘要、大小、创建时间以及更新时间，标签。
// 2、这些数据都是从BoltDB当中直接获取的,没有什么难度,桶路径为：/v1/<namespace>/content/blob/<digest>
type Info struct {
	Digest    digest.Digest // 这个属性相当于镜像层的ID，其实就是镜像层的摘要，一般长这样：sha256:7173b809ca12ec5dee4506cd86be934c4596dd234ee82c0662eac04a8c2c71dc
	Size      int64         // 桶路径为：/v1/<namespace>/content/blob/<digest>, key为：size
	CreatedAt time.Time     // 桶路径为：/v1/<namespace>/content/blob/<digest>, key为：createdat
	UpdatedAt time.Time     // 桶路径为：/v1/<namespace>/content/blob/<digest>, key为：updatedat
	// 桶路径为：/v1/<namespace>/content/blob/<digest>/labels, 这个桶下的所有数据都是标签
	// 目前主要有containerd.io/distribution.source.k8s.mirror.nju.edu.cn以及containerd.io/uncompressed标签
	Labels map[string]string
}

// Status of a content operation (i.e. an ingestion)
// Status主要是用于抽象当前Ingest下载情况
type Status struct {
	// 1、通过读取/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>/ref文件获取值
	// 2、格式类似：default/1/layer-sha256:33486cc813b57bab328443c4145fb29da20d7e6a1dda857716e2be590445cbba
	Ref string
	// 1、通过读取/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>/data文件的大小
	// 2、当前已经保存的文件的大小，通过这个参数可以实现断点续传
	Offset int64
	// 1、通过读取/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>/total文件获取值
	// 2、用于记录当前要下载文件的总大小
	Total int64
	// 当前文件的摘要
	Expected digest.Digest
	// 1、通过读取/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>/startedat文件获取值
	// 2、记录文件的创建时间
	StartedAt time.Time
	// 1、通过读取/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>/updatedat文件获取值
	// 2、记录文件的修改时间
	UpdatedAt time.Time
}

// WalkFunc defines the callback for a blob walk.
type WalkFunc func(Info) error

// Manager provides methods for inspecting, listing and removing content.
// Manager实际上就是对于镜像层获取信息、修改信息、遍历镜像层以及删除镜像层的封装
type Manager interface {
	// Info will return metadata about content available in the content store.
	//
	// If the content is not present, ErrNotFound will be returned.
	// 1、根据摘获取对应的blob的信息，主要是大小、创建时间、更新时间、标签信息，dgst相当于镜像层的ID
	// 2、这里返回的Info信息其实现有两种方式，一种是直接读取操作系统中的文件的信息，一种是通过读取boltdb metadata源数据
	Info(ctx context.Context, dgst digest.Digest) (Info, error)

	// Update updates mutable information related to content.
	// If one or more fieldpaths are provided, only those
	// fields will be updated.
	// Mutable fields:
	//  labels.*
	// 更新镜像层的标签信息，主要是更新镜像层的标签信息
	Update(ctx context.Context, info Info, fieldpaths ...string) (Info, error)

	// Walk will call fn for each item in the content store which
	// match the provided filters. If no filters are given all
	// items will be walked.
	// 遍历containerd存储的镜像层，并根据指定的过滤器过滤不满足要求的镜像层，这里的过滤器可以根据摘要、标签或者大小，不过根据源码显示
	// 根据大小过滤以及根据标签过滤并没有实现
	Walk(ctx context.Context, fn WalkFunc, filters ...string) error

	// Delete removes the content from the store.
	// 根据摘要删除某个镜像层
	Delete(ctx context.Context, dgst digest.Digest) error
}

// Writer handles writing of content into a content store
type Writer interface {
	// Close closes the writer, if the writer has not been
	// committed this allows resuming or aborting.
	// Calling Close on a closed writer will not error.
	io.WriteCloser

	// Digest may return empty digest or panics until committed.
	Digest() digest.Digest

	// Commit commits the blob (but no roll-back is guaranteed on an error).
	// size and expected can be zero-value when unknown.
	// Commit always closes the writer, even on error.
	// ErrAlreadyExists aborts the writer.
	// 执行此动作时，表明镜像已经下载完成，主要是把/var/lib/containerd/io.containerd.content.v1.content/ingest/<digest>目录中的数据拷贝到
	// /var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>当中
	Commit(ctx context.Context, size int64, expected digest.Digest, opts ...Opt) error

	// Status returns the current state of write
	Status() (Status, error)

	// Truncate updates the size of the target blob
	Truncate(size int64) error
}

// Opt is used to alter the mutable properties of content
type Opt func(*Info) error

// WithLabels allows labels to be set on content
func WithLabels(labels map[string]string) Opt {
	return func(info *Info) error {
		info.Labels = labels
		return nil
	}
}

// WriterOpts is internally used by WriterOpt.
type WriterOpts struct {
	// TODO 这里的ref的格式大概长什么样子？
	Ref string
	// OCI规范中的描述符
	Desc ocispec.Descriptor
}

// WriterOpt is used for passing options to Ingester.Writer.
type WriterOpt func(*WriterOpts) error

// WithDescriptor specifies an OCI descriptor.
// Writer may optionally use the descriptor internally for resolving
// the location of the actual data.
// Write does not require any field of desc to be set.
// If the data size is unknown, desc.Size should be set to 0.
// Some implementations may also accept negative values as "unknown".
func WithDescriptor(desc ocispec.Descriptor) WriterOpt {
	return func(opts *WriterOpts) error {
		opts.Desc = desc
		return nil
	}
}

// WithRef specifies a ref string.
func WithRef(ref string) WriterOpt {
	return func(opts *WriterOpts) error {
		opts.Ref = ref
		return nil
	}
}
