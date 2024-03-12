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

package plugin

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/events/exchange"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// InitContext is used for plugin initialization
// 插件初始化上下文
type InitContext struct {
	Context           context.Context
	Root              string        // 当前插件数据存放位置，默认为：/var/lib/containerd/<plugin-type>.<plugin-id>
	State             string        // 当前插件socket文件存放位置，默认为：/run/containerd/<plugin-type>.<plugin-id>
	Config            interface{}   // 插件的配置信息，改属性是通过Registration注册信息中的Config反序列化而来的
	Address           string        // 插件监听地址
	TTRPCAddress      string        // 插件的TTRPC监听地址
	RegisterReadiness func() func() // TODO 如何理解这里的配置？

	// deprecated: will be removed in 2.0, use plugin.EventType
	// TODO 这玩意是怎么设计的？用来干嘛的？
	Events *exchange.Exchange

	// TODO 如何理解这里Meta的设计  用于在多个插件之间共享数据
	Meta *Meta // plugins can fill in metadata at init.

	// 包含containerd当前所有注册的插件
	plugins *Set
}

// NewContext returns a new plugin InitContext
// 实例化当前插件的初始化上下文
func NewContext(ctx context.Context, r *Registration, plugins *Set, root, state string) *InitContext {
	return &InitContext{
		Context: ctx,
		// 注意，这里修改了每个插件的root目录，规则为：<root>/<plugin-type>.<plugin-id>，譬如/var/lib/containerd/io.containerd.content.v1.content
		Root: filepath.Join(root, r.URI()),
		// <state>/<plugin-type>.<plugin-id>，譬如/run/containerd/io.containerd.content.v1.content
		State: filepath.Join(state, r.URI()),
		// 元数据，用于在多个插件之间共享数据
		Meta: &Meta{
			Exports: map[string]string{},
		},
		plugins: plugins,
	}
}

// Get returns the first plugin by its type
func (i *InitContext) Get(t Type) (interface{}, error) {
	return i.plugins.Get(t)
}

// Meta contains information gathered from the registration and initialization
// process.
type Meta struct {
	Platforms []ocispec.Platform // platforms supported by plugin
	// 用于对外暴露数据，组件可以从里面取出自己感兴趣的数据，譬如root=/var/lib/containerd
	Exports      map[string]string // values exported by plugin
	Capabilities []string          // feature switches for plugin
}

// Plugin represents an initialized plugin, used with an init context.
// 用于抽象当前已经初始化好的插件
type Plugin struct {
	// 插件的注册信息
	Registration *Registration // registration, as initialized
	// 插件的配置
	Config interface{} // config, as initialized
	// 一些元数据，插件之间可以共享这些元数据
	Meta *Meta

	// 插件实例
	instance interface{}
	// 插件初始化错误
	err error // will be set if there was an error initializing the plugin
}

// Err returns the errors during initialization.
// returns nil if not error was encountered
func (p *Plugin) Err() error {
	return p.err
}

// Instance returns the instance and any initialization error of the plugin
func (p *Plugin) Instance() (interface{}, error) {
	return p.instance, p.err
}

// Set defines a plugin collection, used with InitContext.
//
// This maintains ordering and unique indexing over the set.
//
// After iteratively instantiating plugins, this set should represent, the
// ordered, initialization set of plugins for a containerd instance.
type Set struct {
	// 保存实例化好的插件
	ordered     []*Plugin // order of initialization
	byTypeAndID map[Type]map[string]*Plugin
}

// NewPluginSet returns an initialized plugin set
func NewPluginSet() *Set {
	return &Set{
		// 注意这里的二层map，并没有进行初始化
		byTypeAndID: make(map[Type]map[string]*Plugin),
	}
}

// Add 保存插件，同时检查插件是否是重复注册
func (ps *Set) Add(p *Plugin) error {
	if byID, typeok := ps.byTypeAndID[p.Registration.Type]; !typeok {
		// 如果当前，还没有注册过这种类型的插件，就直接实例化一个map，保存这种类型的插件
		ps.byTypeAndID[p.Registration.Type] = map[string]*Plugin{
			p.Registration.ID: p,
		}
	} else if _, idok := byID[p.Registration.ID]; !idok {
		// 如果当前保存过这种类型的插件，并且这个插件之前没有注册过，那么注册当前插件
		byID[p.Registration.ID] = p
	} else {
		return fmt.Errorf("plugin %v already initialized: %w", p.Registration.URI(), errdefs.ErrAlreadyExists)
	}

	ps.ordered = append(ps.ordered, p)
	return nil
}

// Get returns the first plugin by its type
// TODO 一个类型的插件可能会有多个，为什么这里直接返回第一个？
func (ps *Set) Get(t Type) (interface{}, error) {
	for _, v := range ps.byTypeAndID[t] {
		return v.Instance()
	}
	return nil, fmt.Errorf("no plugins registered for %s: %w", t, errdefs.ErrNotFound)
}

// GetAll returns all initialized plugins
func (ps *Set) GetAll() []*Plugin {
	return ps.ordered
}

// Plugins returns plugin set
func (i *InitContext) Plugins() *Set {
	return i.plugins
}

// GetAll plugins in the set
func (i *InitContext) GetAll() []*Plugin {
	return i.plugins.GetAll()
}

// GetByID returns the plugin of the given type and ID
func (i *InitContext) GetByID(t Type, id string) (interface{}, error) {
	ps, err := i.GetByType(t)
	if err != nil {
		return nil, err
	}
	p, ok := ps[id]
	if !ok {
		return nil, fmt.Errorf("no %s plugins with id %s: %w", t, id, errdefs.ErrNotFound)
	}
	return p.Instance()
}

// GetByType returns all plugins with the specific type.
func (i *InitContext) GetByType(t Type) (map[string]*Plugin, error) {
	p, ok := i.plugins.byTypeAndID[t]
	if !ok {
		return nil, fmt.Errorf("no plugins registered for %s: %w", t, errdefs.ErrNotFound)
	}

	return p, nil
}
