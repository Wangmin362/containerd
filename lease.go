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

package containerd

import (
	"context"
	"time"

	"github.com/containerd/containerd/leases"
)

// WithLease attaches a lease on the context
func (c *Client) WithLease(ctx context.Context, opts ...leases.Opt) (context.Context, func(context.Context) error, error) {
	nop := func(context.Context) error { return nil }

	// 从上下文中获取租约
	_, ok := leases.FromContext(ctx)
	if ok {
		return ctx, nop, nil
	}

	// 获取租约管理器
	ls := c.LeasesService()

	if len(opts) == 0 {
		// Use default lease configuration if no options provided
		opts = []leases.Opt{
			leases.WithRandomID(),
			leases.WithExpiration(24 * time.Hour),
		}
	}

	// 直接创建一个租约
	l, err := ls.Create(ctx, opts...)
	if err != nil {
		return ctx, nop, err
	}

	// 把租约放到上下文当中
	ctx = leases.WithLease(ctx, l.ID)
	return ctx, func(ctx context.Context) error {
		// 删除租约
		return ls.Delete(ctx, l)
	}, nil
}
