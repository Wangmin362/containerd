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

package images

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/containerd/containerd/content"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/platforms"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Image provides the model for how containerd views container images.
type Image struct {
	// Name of the image.
	//
	// To be pulled, it must be a reference compatible with resolvers.
	//
	// This field is required.
	Name string

	// Labels provide runtime decoration for the image record.
	//
	// There is no default behavior for how these labels are propagated. They
	// only decorate the static metadata object.
	//
	// This field is optional.
	Labels map[string]string

	// Target describes the root content for this image. Typically, this is
	// a manifest, index or manifest list.
	// 所谓的Target，其实就是Manifest或者Image Index文件，通过Target可以找到镜像的完整数据
	Target ocispec.Descriptor

	CreatedAt, UpdatedAt time.Time
}

// DeleteOptions provide options on image delete
type DeleteOptions struct {
	Synchronous bool
}

// DeleteOpt allows configuring a delete operation
type DeleteOpt func(context.Context, *DeleteOptions) error

// SynchronousDelete is used to indicate that an image deletion and removal of
// the image resources should occur synchronously before returning a result.
func SynchronousDelete() DeleteOpt {
	return func(ctx context.Context, o *DeleteOptions) error {
		o.Synchronous = true
		return nil
	}
}

// Store and interact with images
type Store interface {
	Get(ctx context.Context, name string) (Image, error)
	List(ctx context.Context, filters ...string) ([]Image, error)
	Create(ctx context.Context, image Image) (Image, error)

	// Update will replace the data in the store with the provided image. If
	// one or more fieldpaths are provided, only those fields will be updated.
	Update(ctx context.Context, image Image, fieldpaths ...string) (Image, error)

	Delete(ctx context.Context, name string, opts ...DeleteOpt) error
}

// TODO(stevvooe): Many of these functions make strong platform assumptions,
// which are untrue in a lot of cases. More refactoring must be done here to
// make this work in all cases.

// Config resolves the image configuration descriptor.
//
// The caller can then use the descriptor to resolve and process the
// configuration of the image.
// 读取镜像的Config文件，该文件的描述符从Manifest中获取
func (image *Image) Config(ctx context.Context, provider content.Provider, platform platforms.MatchComparer) (ocispec.Descriptor, error) {
	// 读取镜像的Config文件，该文件的描述符从Manifest中获取
	return Config(ctx, provider, image.Target, platform)
}

// RootFS returns the unpacked diffids that make up and images rootfs.
//
// These are used to verify that a set of layers unpacked to the expected
// values.
// 获取镜像的RootFs.DiffID，原理如下：
// 1、根据镜像层的摘要信息读取Manifest
// 2、从Manifest获取Config的Digest
// 3、根据获取到的Config.Digest读取Config文件
// 4、获取RootFS.DiffIDs
func (image *Image) RootFS(ctx context.Context, provider content.Provider, platform platforms.MatchComparer) ([]digest.Digest, error) {
	// 读取镜像的Config文件，该文件的描述符从Manifest中获取
	desc, err := image.Config(ctx, provider, platform)
	if err != nil {
		return nil, err
	}
	// 从镜像的Config文件中获取DiffID
	return RootFS(ctx, provider, desc)
}

// Size returns the total size of an image's packed resources.
func (image *Image) Size(ctx context.Context, provider content.Provider, platform platforms.MatchComparer) (int64, error) {
	var size int64
	return size, Walk(ctx, Handlers(HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		if desc.Size < 0 {
			return nil, fmt.Errorf("invalid size %v in %v (%v)", desc.Size, desc.Digest, desc.MediaType)
		}
		size += desc.Size
		return nil, nil
	}), LimitManifests(FilterPlatforms(ChildrenHandler(provider), platform), platform, 1)), image.Target)
}

type platformManifest struct {
	p *ocispec.Platform
	m *ocispec.Manifest
}

// Manifest resolves a manifest from the image for the given platform.
//
// When a manifest descriptor inside of a manifest index does not have
// a platform defined, the platform from the image config is considered.
//
// If the descriptor points to a non-index manifest, then the manifest is
// unmarshalled and returned without considering the platform inside of the
// config.
//
// TODO(stevvooe): This violates the current platform agnostic approach to this
// package by returning a specific manifest type. We'll need to refactor this
// to return a manifest descriptor or decide that we want to bring the API in
// this direction because this abstraction is not needed.
// 读取镜像的Manifest，读取位置为：/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
func Manifest(ctx context.Context, provider content.Provider, image ocispec.Descriptor, platform platforms.MatchComparer) (ocispec.Manifest, error) {
	var (
		limit    = 1
		m        []platformManifest
		wasIndex bool
	)

	// TODO 这里的递归遍历的写法也很值得学习
	if err := Walk(ctx, HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		switch desc.MediaType {
		// 如果当前文件是Manifest文件，那么此文件将会包含镜像的Config以及Layer
		case MediaTypeDockerSchema2Manifest, ocispec.MediaTypeImageManifest:
			// 当前镜像层保存位置为：/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
			p, err := content.ReadBlob(ctx, provider, desc)
			if err != nil {
				return nil, err
			}

			// 校验当前的MediaType,如果MediaType不对，那么反序列化必然失败
			if err := validateMediaType(p, desc.MediaType); err != nil {
				return nil, fmt.Errorf("manifest: invalid desc %s: %w", desc.Digest, err)
			}

			var manifest ocispec.Manifest
			if err := json.Unmarshal(p, &manifest); err != nil {
				return nil, err
			}

			if desc.Digest != image.Digest && platform != nil {
				if desc.Platform != nil && !platform.Match(*desc.Platform) {
					return nil, nil
				}

				if desc.Platform == nil {
					// 如果镜像的config文件
					p, err := content.ReadBlob(ctx, provider, manifest.Config)
					if err != nil {
						return nil, err
					}

					var image ocispec.Image
					if err := json.Unmarshal(p, &image); err != nil {
						return nil, err
					}

					if !platform.Match(platforms.Normalize(ocispec.Platform{OS: image.OS, Architecture: image.Architecture})) {
						return nil, nil
					}

				}
			}

			m = append(m, platformManifest{
				p: desc.Platform,
				m: &manifest,
			})

			return nil, nil
		// 如果当前镜像层是image index文件，在Docker中被称为ManifestList，该文件用于支持镜像的多平台
		case MediaTypeDockerSchema2ManifestList, ocispec.MediaTypeImageIndex:
			// 当前镜像层保存位置为：/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>
			p, err := content.ReadBlob(ctx, provider, desc)
			if err != nil {
				return nil, err
			}

			// 校验媒体类型，如果媒体类型不对，那么反序列化必然失败
			if err := validateMediaType(p, desc.MediaType); err != nil {
				return nil, fmt.Errorf("manifest: invalid desc %s: %w", desc.Digest, err)
			}

			var idx ocispec.Index
			// 反序列化
			if err := json.Unmarshal(p, &idx); err != nil {
				return nil, err
			}

			// 没有指定平台的话，就返回所有平台的镜像
			if platform == nil {
				return idx.Manifests, nil
			}

			// 否则返回满足当前平台的镜像层摘要，实际上这个镜像层被称为manifest
			var descs []ocispec.Descriptor
			for _, d := range idx.Manifests {
				if d.Platform == nil || platform.Match(*d.Platform) {
					descs = append(descs, d)
				}
			}

			sort.SliceStable(descs, func(i, j int) bool {
				if descs[i].Platform == nil {
					return false
				}
				if descs[j].Platform == nil {
					return true
				}
				return platform.Less(*descs[i].Platform, *descs[j].Platform)
			})

			wasIndex = true

			if len(descs) > limit {
				return descs[:limit], nil
			}
			return descs, nil
		}
		return nil, fmt.Errorf("unexpected media type %v for %v: %w", desc.MediaType, desc.Digest, errdefs.ErrNotFound)
	}), image); err != nil {
		return ocispec.Manifest{}, err
	}

	if len(m) == 0 {
		err := fmt.Errorf("manifest %v: %w", image.Digest, errdefs.ErrNotFound)
		if wasIndex {
			err = fmt.Errorf("no match for platform in manifest %v: %w", image.Digest, errdefs.ErrNotFound)
		}
		return ocispec.Manifest{}, err
	}
	return *m[0].m, nil
}

// Config resolves the image configuration descriptor using a content provided
// to resolve child resources on the image.
//
// The caller can then use the descriptor to resolve and process the
// configuration of the image.
// 读取镜像的Config文件，该文件的描述符从Manifest中获取
func Config(ctx context.Context, provider content.Provider, image ocispec.Descriptor, platform platforms.MatchComparer) (ocispec.Descriptor, error) {
	// 读取镜像的Manifest
	manifest, err := Manifest(ctx, provider, image, platform)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	return manifest.Config, err
}

// Platforms returns one or more platforms supported by the image.
func Platforms(ctx context.Context, provider content.Provider, image ocispec.Descriptor) ([]ocispec.Platform, error) {
	var platformSpecs []ocispec.Platform
	return platformSpecs, Walk(ctx, Handlers(HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		if desc.Platform != nil {
			platformSpecs = append(platformSpecs, *desc.Platform)
			return nil, ErrSkipDesc
		}

		switch desc.MediaType {
		case MediaTypeDockerSchema2Config, ocispec.MediaTypeImageConfig:
			p, err := content.ReadBlob(ctx, provider, desc)
			if err != nil {
				return nil, err
			}

			var image ocispec.Image
			if err := json.Unmarshal(p, &image); err != nil {
				return nil, err
			}

			platformSpecs = append(platformSpecs,
				platforms.Normalize(ocispec.Platform{OS: image.OS, Architecture: image.Architecture}))
		}
		return nil, nil
	}), ChildrenHandler(provider)), image)
}

// Check returns nil if the all components of an image are available in the
// provider for the specified platform.
//
// If available is true, the caller can assume that required represents the
// complete set of content required for the image.
//
// missing will have the components that are part of required but not available
// in the provider.
//
// If there is a problem resolving content, an error will be returned.
func Check(ctx context.Context, provider content.Provider, image ocispec.Descriptor, platform platforms.MatchComparer) (available bool, required, present, missing []ocispec.Descriptor, err error) {
	mfst, err := Manifest(ctx, provider, image, platform)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return false, []ocispec.Descriptor{image}, nil, []ocispec.Descriptor{image}, nil
		}

		return false, nil, nil, nil, fmt.Errorf("failed to check image %v: %w", image.Digest, err)
	}

	// TODO(stevvooe): It is possible that referenced components could have
	// children, but this is rare. For now, we ignore this and only verify
	// that manifest components are present.
	required = append([]ocispec.Descriptor{mfst.Config}, mfst.Layers...)

	for _, desc := range required {
		ra, err := provider.ReaderAt(ctx, desc)
		if err != nil {
			if errdefs.IsNotFound(err) {
				missing = append(missing, desc)
				continue
			} else {
				return false, nil, nil, nil, fmt.Errorf("failed to check image %v: %w", desc.Digest, err)
			}
		}
		ra.Close()
		present = append(present, desc)

	}

	return true, required, present, missing, nil
}

// Children returns the immediate children of content described by the descriptor.
// 1、一个ocispec.Descriptor实际上就是一个镜像层
// 2、Children用于从Manifest文件或者Index文件中读取当前镜像的镜像层信息
func Children(ctx context.Context, provider content.Provider, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
	var descs []ocispec.Descriptor
	switch desc.MediaType {
	case MediaTypeDockerSchema2Manifest, ocispec.MediaTypeImageManifest:
		// 如果镜像描述符中携带了镜像数据，那么直接返回，如果没有，那么直接从磁盘当中读取镜像信息。
		// 读取的目录位置为：/var/lib/containerd/io.containerd.content.v1.content/blobs
		p, err := content.ReadBlob(ctx, provider, desc)
		if err != nil {
			return nil, err
		}

		// 校验镜像的MediaType
		if err := validateMediaType(p, desc.MediaType); err != nil {
			return nil, fmt.Errorf("children: invalid desc %s: %w", desc.Digest, err)
		}

		// TODO(stevvooe): We just assume oci manifest, for now. There may be
		// subtle differences from the docker version.
		var manifest ocispec.Manifest
		// 反序列化
		if err := json.Unmarshal(p, &manifest); err != nil {
			return nil, err
		}

		descs = append(descs, manifest.Config)
		descs = append(descs, manifest.Layers...)
	case MediaTypeDockerSchema2ManifestList, ocispec.MediaTypeImageIndex:
		// 如果镜像描述符中携带了镜像数据，那么直接返回，如果没有，那么直接从磁盘当中读取镜像信息。
		// 读取的目录位置为：/var/lib/containerd/io.containerd.content.v1.content/blobs
		p, err := content.ReadBlob(ctx, provider, desc)
		if err != nil {
			return nil, err
		}

		// 校验当前镜像层的MediaType
		if err := validateMediaType(p, desc.MediaType); err != nil {
			return nil, fmt.Errorf("children: invalid desc %s: %w", desc.Digest, err)
		}

		var index ocispec.Index
		// 反序列化
		if err := json.Unmarshal(p, &index); err != nil {
			return nil, err
		}

		descs = append(descs, index.Manifests...)
	default:
		if IsLayerType(desc.MediaType) || IsKnownConfig(desc.MediaType) {
			// childless data types.
			return nil, nil
		}
		log.G(ctx).Debugf("encountered unknown type %v; children may not be fetched", desc.MediaType)
	}

	return descs, nil
}

// unknownDocument represents a manifest, manifest list, or index that has not
// yet been validated.
type unknownDocument struct {
	MediaType string          `json:"mediaType,omitempty"`
	Config    json.RawMessage `json:"config,omitempty"`
	Layers    json.RawMessage `json:"layers,omitempty"`
	Manifests json.RawMessage `json:"manifests,omitempty"`
	FSLayers  json.RawMessage `json:"fsLayers,omitempty"` // schema 1
}

// validateMediaType returns an error if the byte slice is invalid JSON or if
// the media type identifies the blob as one format but it contains elements of
// another format.
func validateMediaType(b []byte, mt string) error {
	var doc unknownDocument
	if err := json.Unmarshal(b, &doc); err != nil {
		return err
	}
	if len(doc.FSLayers) != 0 {
		return fmt.Errorf("media-type: schema 1 not supported")
	}
	switch mt {
	case MediaTypeDockerSchema2Manifest, ocispec.MediaTypeImageManifest:
		if len(doc.Manifests) != 0 ||
			doc.MediaType == MediaTypeDockerSchema2ManifestList ||
			doc.MediaType == ocispec.MediaTypeImageIndex {
			return fmt.Errorf("media-type: expected manifest but found index (%s)", mt)
		}
	case MediaTypeDockerSchema2ManifestList, ocispec.MediaTypeImageIndex:
		if len(doc.Config) != 0 || len(doc.Layers) != 0 ||
			doc.MediaType == MediaTypeDockerSchema2Manifest ||
			doc.MediaType == ocispec.MediaTypeImageManifest {
			return fmt.Errorf("media-type: expected index but found manifest (%s)", mt)
		}
	}
	return nil
}

// RootFS returns the unpacked diffids that make up and images rootfs.
//
// These are used to verify that a set of layers unpacked to the expected
// values.
// 从镜像的Config文件中获取DiffID
func RootFS(ctx context.Context, provider content.Provider, configDesc ocispec.Descriptor) ([]digest.Digest, error) {
	// 读取/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256/<digest>文件
	// 实际上这里读取的就是镜像的config信息
	p, err := content.ReadBlob(ctx, provider, configDesc)
	if err != nil {
		return nil, err
	}

	// TODO 既然其它地方都做了MediaType校验，那么这里是不是也应该做校验，不然反序列化必然有问题

	var config ocispec.Image
	if err := json.Unmarshal(p, &config); err != nil {
		return nil, err
	}
	return config.RootFS.DiffIDs, nil
}
