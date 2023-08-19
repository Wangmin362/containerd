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

package rootfs

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/containerd/containerd/diff"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/image-spec/identity"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// Layer represents the descriptors for a layer diff. These descriptions
// include the descriptor for the uncompressed tar diff as well as a blob
// used to transport that tar. The blob descriptor may or may not describe
// a compressed object.
// TODO 所谓的diff_id，其实就是镜像层未压缩之前的摘要
type Layer struct {
	Diff ocispec.Descriptor // image config中的diff id
	Blob ocispec.Descriptor // 磁盘中存储的镜像层文件，位置在/var/lib/containerd/io.containerd.content.v1.content/blobs/sha256
}

// ApplyLayers applies all the layers using the given snapshotter and applier.
// The returned result is a chain id digest representing all the applied layers.
// Layers are applied in order they are given, making the first layer the
// bottom-most layer in the layer chain.
func ApplyLayers(ctx context.Context, layers []Layer, sn snapshots.Snapshotter, a diff.Applier) (digest.Digest, error) {
	return ApplyLayersWithOpts(ctx, layers, sn, a, nil)
}

// ApplyLayersWithOpts applies all the layers using the given snapshotter, applier, and apply opts.
// The returned result is a chain id digest representing all the applied layers.
// Layers are applied in order they are given, making the first layer the
// bottom-most layer in the layer chain.
func ApplyLayersWithOpts(ctx context.Context, layers []Layer, sn snapshots.Snapshotter, a diff.Applier, applyOpts []diff.ApplyOpt) (digest.Digest, error) {
	chain := make([]digest.Digest, len(layers))
	for i, layer := range layers {
		chain[i] = layer.Diff.Digest
	}
	chainID := identity.ChainID(chain)

	// Just stat top layer, remaining layers will have their existence checked
	// on prepare. Calling prepare on upper layers first guarantees that upper
	// layers are not removed while calling stat on lower layers
	_, err := sn.Stat(ctx, chainID.String())
	if err != nil {
		if !errdefs.IsNotFound(err) {
			return "", fmt.Errorf("failed to stat snapshot %s: %w", chainID, err)
		}

		if err := applyLayers(ctx, layers, chain, sn, a, nil, applyOpts); err != nil && !errdefs.IsAlreadyExists(err) {
			return "", err
		}
	}

	return chainID, nil
}

// ApplyLayer applies a single layer on top of the given provided layer chain,
// using the provided snapshotter and applier. If the layer was unpacked true
// is returned, if the layer already exists false is returned.
func ApplyLayer(ctx context.Context, layer Layer, chain []digest.Digest, sn snapshots.Snapshotter, a diff.Applier, opts ...snapshots.Opt) (bool, error) {
	return ApplyLayerWithOpts(ctx, layer, chain, sn, a, opts, nil)
}

// ApplyLayerWithOpts applies a single layer on top of the given provided layer chain,
// using the provided snapshotter, applier, and apply opts. If the layer was unpacked true
// is returned, if the layer already exists false is returned.
// 1、如果当前镜像层未解压，返回true；如果当前层已经存在，返回false  2、这里的snapshotter其实就是元数据服务
func ApplyLayerWithOpts(ctx context.Context, layer Layer, chain []digest.Digest, sn snapshots.Snapshotter, a diff.Applier, opts []snapshots.Opt, applyOpts []diff.ApplyOpt) (bool, error) {
	var (
		// chainID的计算方法：
		/*
			ChainID(L₀) =  DiffID(L₀)
			ChainID(L₀|...|Lₙ₋₁|Lₙ) = Digest(ChainID(L₀|...|Lₙ₋₁) + " " + DiffID(Lₙ))
		*/
		// 简单来说：ChainID0 = DiffID0  ChainID1 = Digest("ChainID0" + " " + DiffID1)
		// ChainID2 = Digest("ChainID1" + " " + DiffID1)   ChainID3 = Digest("ChainID2" + " " + DiffID3)
		// 利用chainID的计算公式计算出当前层的chainID
		chainID = identity.ChainID(append(chain, layer.Diff.Digest)).String()
		applied bool
	)
	// 1、通过metadata服务获取镜像层的元数据信息，数据库为：/var/lib/containerd/io.containerd.metadata.v1.bolt/meta.db
	// 路径为：/v1/<namespace>/snapshots/<snapshot>/<diff_id>
	if _, err := sn.Stat(ctx, chainID); err != nil {
		if !errdefs.IsNotFound(err) {
			return false, fmt.Errorf("failed to stat snapshot %s: %w", chainID, err)
		}

		if err := applyLayers(ctx, []Layer{layer}, append(chain, layer.Diff.Digest), sn, a, opts, applyOpts); err != nil {
			if !errdefs.IsAlreadyExists(err) {
				return false, err
			}
		} else {
			applied = true
		}
	}
	return applied, nil

}

func applyLayers(ctx context.Context, layers []Layer, chain []digest.Digest, sn snapshots.Snapshotter, a diff.Applier, opts []snapshots.Opt, applyOpts []diff.ApplyOpt) error {
	var (
		parent  = identity.ChainID(chain[:len(chain)-1]) // 除了最底层没有parent,其余层均有parent
		chainID = identity.ChainID(chain)
		layer   = layers[len(layers)-1] // 获取最后一层  TODO 为啥是获取最后一层
		diff    ocispec.Descriptor
		key     string
		mounts  []mount.Mount
		err     error
	)

	for {
		// key = "extract-706323134-H4Kh sha256:d0157aa0c95a4cae128dab97d699b2f303c8bea46914dc4a40722411f50bb40e"
		key = fmt.Sprintf(snapshots.UnpackKeyFormat, uniquePart(), chainID)

		// Prepare snapshot with from parent, label as root
		// 所谓的创建快照，其实主要做了两件事情：
		// 一：创建v1/<namespace>/snapshots/<snapshots>桶，并在其中记录元信息
		// 二：创建/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/<id>目录，并在目录中创建fs, work目录
		// 返回值为mount挂载参数
		mounts, err = sn.Prepare(ctx, key, parent.String(), opts...)
		if err != nil {
			if errdefs.IsNotFound(err) && len(layers) > 1 {
				if err := applyLayers(ctx, layers[:len(layers)-1], chain[:len(chain)-1], sn, a, opts, applyOpts); err != nil {
					if !errdefs.IsAlreadyExists(err) {
						return err
					}
				}
				// Do no try applying layers again
				layers = nil
				continue
			} else if errdefs.IsAlreadyExists(err) {
				// Try a different key
				continue
			}

			// Already exists should have the caller retry
			return fmt.Errorf("failed to prepare extraction snapshot %q: %w", key, err)

		}
		break
	}
	defer func() {
		if err != nil {
			if !errdefs.IsAlreadyExists(err) {
				log.G(ctx).WithError(err).WithField("key", key).Infof("apply failure, attempting cleanup")
			}

			if rerr := sn.Remove(ctx, key); rerr != nil {
				log.G(ctx).WithError(rerr).WithField("key", key).Warnf("extraction snapshot removal failed")
			}
		}
	}()

	diff, err = a.Apply(ctx, layer.Blob, mounts, applyOpts...)
	if err != nil {
		err = fmt.Errorf("failed to extract layer %s: %w", layer.Diff.Digest, err)
		return err
	}
	if diff.Digest != layer.Diff.Digest {
		err = fmt.Errorf("wrong diff id calculated on extraction %q", diff.Digest)
		return err
	}

	if err = sn.Commit(ctx, chainID.String(), key, opts...); err != nil {
		err = fmt.Errorf("failed to commit snapshot %s: %w", key, err)
		return err
	}

	return nil
}

func uniquePart() string {
	t := time.Now()
	var b [3]byte
	// Ignore read failures, just decreases uniqueness
	rand.Read(b[:])
	return fmt.Sprintf("%d-%s", t.Nanosecond(), base64.URLEncoding.EncodeToString(b[:]))
}
