package badgerdb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/alash3al/goukv"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

// Provider represents a provider
type Provider struct {
	db *badger.DB
}

// Open implements goukv.Open
func (p Provider) Open(opts map[string]interface{}) (goukv.Provider, error) {
	path, ok := opts["path"].(string)
	if !ok {
		return nil, errors.New("must specify path")
	}

	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	syncWrites, ok := opts["sync_writes"].(bool)
	if !ok {
		syncWrites = false
	}

	badgerOpts := badger.DefaultOptions(path)

	badgerOpts.WithSyncWrites(syncWrites)
	badgerOpts.WithLogger(nil)
	badgerOpts.WithKeepL0InMemory(true)
	badgerOpts.WithCompression(options.Snappy)

	db, err := badger.Open(badgerOpts)
	if err != nil {
		return nil, err
	}

	go (func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			for {
				err := db.RunValueLogGC(0.5)
				if err != nil {
					break
				}
			}
		}
	})()

	return &Provider{
		db: db,
	}, nil
}

// Put implements goukv.Put
func (p Provider) Put(entry *goukv.Entry) error {
	return p.db.Update(func(txn *badger.Txn) error {
		if entry.TTL > 0 {
			badgerEntry := badger.NewEntry(entry.Key, entry.Value)
			badgerEntry.WithTTL(entry.TTL)
			return txn.SetEntry(badgerEntry)
		}

		return txn.Set(entry.Key, entry.Value)
	})
}

// Batch perform multi put operation, empty value means *delete*
func (p Provider) Batch(entries []*goukv.Entry) error {
	batch := p.db.NewWriteBatch()
	defer batch.Cancel()

	for _, entry := range entries {
		var err error
		if entry.Value == nil {
			err = batch.Delete(entry.Key)
		} else {
			if entry.TTL > 0 {
				badgerEntry := badger.NewEntry(entry.Key, entry.Value)
				badgerEntry.WithTTL(entry.TTL)

				err = batch.SetEntry(badgerEntry)
			} else {
				err = batch.Set(entry.Key, entry.Value)
			}
		}

		if err != nil {
			batch.Cancel()
			return err
		}
	}

	return batch.Flush()
}

// Get implements goukv.Get
func (p Provider) Get(k []byte) ([]byte, error) {
	var data []byte
	err := p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err == badger.ErrKeyNotFound {
			return goukv.ErrKeyNotFound
		}

		if err != nil {
			return err
		}

		d, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		data = d

		return err
	})

	return data, err
}

// TTL implements goukv.TTL
func (p Provider) TTL(k []byte) (*time.Time, error) {
	var t *time.Time
	err := p.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err == badger.ErrKeyNotFound {
			return goukv.ErrKeyNotFound
		}

		if err != nil {
			return err
		}

		expiresAt := item.ExpiresAt()
		if expiresAt > 0 {
			toUnix := time.Unix(int64(expiresAt), 0)
			t = &toUnix
		}

		return err
	})

	return t, err
}

// Delete implements goukv.Delete
func (p Provider) Delete(k []byte) error {
	return p.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
}

// Close implements goukv.Close
func (p Provider) Close() error {
	return p.db.Close()
}

// Scan implements goukv.Scan
func (p Provider) Scan(opts goukv.ScanOpts) error {
	if opts.Scanner == nil {
		return goukv.ErrNoScanner
	}

	txn := p.db.NewTransaction(false)
	defer txn.Commit()

	iterOpts := badger.DefaultIteratorOptions
	iterOpts.Reverse = opts.ReverseScan

	if len(opts.Prefix) > 0 {
		iterOpts.Prefix = opts.Prefix
	}

	iter := txn.NewIterator(iterOpts)
	defer iter.Close()

	if opts.Offset != nil {
		iter.Seek(opts.Offset)
	} else {
		iter.Rewind()
	}

	checked := false
	for ; iter.Valid(); iter.Next() {
		item := iter.Item()

		key := item.KeyCopy(nil)
		if !checked && opts.Offset != nil && !opts.IncludeOffset && bytes.Compare(key, opts.Offset) == 0 {
			checked = true
			continue
		}
		checked = true

		val, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		if err := opts.Scanner(key, val); err != nil {
			if err == goukv.ErrScanDone {
				break
			}
			return err
		}
	}
	return nil
}
