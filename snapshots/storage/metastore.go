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

// Package storage provides a metadata storage implementation for snapshot
// drivers. Drive implementations are responsible for starting and managing
// transactions using the defined context creator. This storage package uses
// BoltDB for storing metadata. Access to the raw boltdb transaction is not
// provided, but the stored object is provided by the proto subpackage.
package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/snapshots"
	"github.com/hashicorp/go-multierror"
	bolt "go.etcd.io/bbolt"
)

// Transactor is used to finalize an active transaction.
type Transactor interface {
	// Commit commits any changes made during the transaction. On error a
	// caller is expected to clean up any resources which would have relied
	// on data mutated as part of this transaction. Only writable
	// transactions can commit, non-writable must call Rollback.
	Commit() error

	// Rollback rolls back any changes made during the transaction. This
	// must be called on all non-writable transactions and aborted writable
	// transaction.
	Rollback() error
}

// Snapshot hold the metadata for an active or view snapshot transaction. The
// ParentIDs hold the snapshot identifiers for the committed snapshots this
// active or view is based on. The ParentIDs are ordered from the lowest base
// to highest, meaning they should be applied in order from the first index to
// the last index. The last index should always be considered the active
// snapshots immediate parent.
type Snapshot struct {
	Kind      snapshots.Kind
	ID        string
	ParentIDs []string
}

/*
- v1
- parents
  010002: default/4/sha256:4393f4a23174c8219b87411a6f1d20f7a7b1bcc5cd5ee2a3e8994bfc7095c614
  020003: default/6/sha256:d6cd83226886b0015ae158eaa533d2054970b6cd7353b69027b73eca8b19f7c9
  030004: default/8/sha256:64248ceb66c340b117152a623ce7e9d1ee82b47cb05b1784a88158926b18d682
  040005: default/10/sha256:fa32510685b5d0f9502eb9a257affc81768b778a7935e74c94820dc6aa52b557
  050006: default/12/sha256:89e899eda66e049f4155e9517ae1e382b07448d8b3a510cfdf78835c6e75d1df
  060008: default/14/69012b2c5cdc12c1b1de1c997f0677308eab8f16b0c925267452ec1b3b138b7f
- snapshots
  - default/10/sha256:fa32510685b5d0f9502eb9a257affc81768b778a7935e74c94820dc6aa52b557
	+ labels
	createdat: 010000000edc5f632d242f116bffff
	id: 05
	inodes: 04
	kind: 03
	parent: default/8/sha256:64248ceb66c340b117152a623ce7e9d1ee82b47cb05b1784a88158926b18d682
	size: 00
	updatedat: 010000000edc5f632d242f116bffff
  + default/12/sha256:89e899eda66e049f4155e9517ae1e382b07448d8b3a510cfdf78835c6e75d1df
  + default/14/69012b2c5cdc12c1b1de1c997f0677308eab8f16b0c925267452ec1b3b138b7f
  + default/2/sha256:b2d5eeeaba3a22b9b8aa97261957974a6bd65274ebd43e1d81d0a7b8b752b116
  + default/4/sha256:4393f4a23174c8219b87411a6f1d20f7a7b1bcc5cd5ee2a3e8994bfc7095c614
  - default/6/sha256:d6cd83226886b0015ae158eaa533d2054970b6cd7353b69027b73eca8b19f7c9
	+ labels
	createdat: 010000000edc5f632d02185c08ffff
	id: 03
	inodes: 940d
	kind: 03
	parent: default/4/sha256:4393f4a23174c8219b87411a6f1d20f7a7b1bcc5cd5ee2a3e8994bfc7095c614
	size: 8080af03
	updatedat: 010000000edc5f632d02185c08ffff
  - default/8/sha256:64248ceb66c340b117152a623ce7e9d1ee82b47cb05b1784a88158926b18d682
	+ labels
	createdat: 010000000edc5f632d23ddac34ffff
	id: 04
	inodes: fe01
	kind: 03
	parent: default/6/sha256:d6cd83226886b0015ae158eaa533d2054970b6cd7353b69027b73eca8b19f7c9
	size: 80c0a318
	updatedat: 010000000edc5f632d23ddac34ffff
*/

// MetaStore is used to store metadata related to a snapshot driver. The
// MetaStore is intended to store metadata related to name, state and
// parentage. Using the MetaStore is not required to implement a snapshot
// driver but can be used to handle the persistence and transactional
// complexities of a driver implementation.
// 1、元数据存储用于存储快照驱动相关的元数据
type MetaStore struct {
	// boltdb存储数据的位置
	dbfile string

	dbL sync.Mutex
	// 核心实现其实就是boLtdb
	db *bolt.DB
}

// NewMetaStore returns a snapshot MetaStore for storage of metadata related to
// a snapshot driver backed by a bolt file database. This implementation is
// strongly consistent and does all metadata changes in a transaction to prevent
// against process crashes causing inconsistent metadata state.
func NewMetaStore(dbfile string) (*MetaStore, error) {
	return &MetaStore{
		dbfile: dbfile,
	}, nil
}

type transactionKey struct{}

// TransactionContext creates a new transaction context. The writable value
// should be set to true for transactions which are expected to mutate data.
func (ms *MetaStore) TransactionContext(ctx context.Context, writable bool) (context.Context, Transactor, error) {
	ms.dbL.Lock()
	if ms.db == nil {
		db, err := bolt.Open(ms.dbfile, 0600, nil)
		if err != nil {
			ms.dbL.Unlock()
			return ctx, nil, fmt.Errorf("failed to open database file: %w", err)
		}
		ms.db = db
	}
	ms.dbL.Unlock()

	// 开启事务
	tx, err := ms.db.Begin(writable)
	if err != nil {
		return ctx, nil, fmt.Errorf("failed to start transaction: %w", err)
	}

	ctx = context.WithValue(ctx, transactionKey{}, tx)

	return ctx, tx, nil
}

// TransactionCallback represents a callback to be invoked while under a metastore transaction.
type TransactionCallback func(ctx context.Context) error

// WithTransaction is a convenience method to run a function `fn` while holding a meta store transaction.
// If the callback `fn` returns an error or the transaction is not writable, the database transaction will be discarded.
func (ms *MetaStore) WithTransaction(ctx context.Context, writable bool, fn TransactionCallback) error {
	ctx, trans, err := ms.TransactionContext(ctx, writable)
	if err != nil {
		return err
	}

	var result *multierror.Error
	err = fn(ctx)
	if err != nil {
		result = multierror.Append(result, err)
	}

	// Always rollback if transaction is not writable
	if err != nil || !writable {
		// 函数执行有问题，就回滚
		if terr := trans.Rollback(); terr != nil {
			log.G(ctx).WithError(terr).Error("failed to rollback transaction")

			result = multierror.Append(result, fmt.Errorf("rollback failed: %w", terr))
		}
	} else {
		// 执行成功就提交
		if terr := trans.Commit(); terr != nil {
			log.G(ctx).WithError(terr).Error("failed to commit transaction")

			result = multierror.Append(result, fmt.Errorf("commit failed: %w", terr))
		}
	}

	if err := result.ErrorOrNil(); err != nil {
		log.G(ctx).WithError(err).Debug("snapshotter error")

		// Unwrap if just one error
		if len(result.Errors) == 1 {
			return result.Errors[0]
		}
		return err
	}

	return nil
}

// Close closes the metastore and any underlying database connections
func (ms *MetaStore) Close() error {
	ms.dbL.Lock()
	defer ms.dbL.Unlock()
	if ms.db == nil {
		return nil
	}
	return ms.db.Close()
}
