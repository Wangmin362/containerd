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

package leases

import (
	"context"
	"time"
)

// Opt is used to set options on a lease
type Opt func(*Lease) error

// DeleteOpt allows configuring a delete operation
type DeleteOpt func(context.Context, *DeleteOptions) error

// Manager is used to create, list, and remove leases
// 实现Lease资源对象的增删改查
type Manager interface {
	// Create 创建lease资源对象
	Create(context.Context, ...Opt) (Lease, error)
	// Delete 删除Lease资源
	Delete(context.Context, Lease, ...DeleteOpt) error
	// List 遍历所有的lease资源对象
	List(context.Context, ...string) ([]Lease, error)
	// AddResource 把Lease资源对象和Resource关联起来
	AddResource(context.Context, Lease, Resource) error
	// DeleteResource 删除Lease资源对象和Resource关联关系
	DeleteResource(context.Context, Lease, Resource) error
	// ListResources 获取lease资源对象的所有关联Resource
	ListResources(context.Context, Lease) ([]Resource, error)
}

// Lease retains resources to prevent cleanup before
// the resources can be fully referenced.
type Lease struct {
	ID        string            // lease id
	CreatedAt time.Time         // lease创建时间
	Labels    map[string]string // lease标签
}

// Resource represents low level resource of image, like content, ingest and
// snapshotter.
type Resource struct {
	ID   string
	Type string
}

// DeleteOptions provide options on image delete
type DeleteOptions struct {
	Synchronous bool
}

// SynchronousDelete is used to indicate that a lease deletion and removal of
// any unreferenced resources should occur synchronously before returning the
// result.
func SynchronousDelete(ctx context.Context, o *DeleteOptions) error {
	o.Synchronous = true
	return nil
}

// WithLabels merges labels on a lease
func WithLabels(labels map[string]string) Opt {
	return func(l *Lease) error {
		if l.Labels == nil {
			l.Labels = map[string]string{}
		}
		for k, v := range labels {
			l.Labels[k] = v
		}
		return nil
	}
}

// WithExpiration sets an expiration on the lease
func WithExpiration(d time.Duration) Opt {
	return func(l *Lease) error {
		if l.Labels == nil {
			l.Labels = map[string]string{}
		}
		l.Labels["containerd.io/gc.expire"] = time.Now().Add(d).Format(time.RFC3339)

		return nil
	}
}
