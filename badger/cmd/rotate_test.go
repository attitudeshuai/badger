/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/y"
)

func TestRotate(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Creating sample key.
	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)

	fp, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fp.Write(key)
	require.NoError(t, err)
	defer fp.Close()

	// Opening DB with the encryption key.
	opts := badger.DefaultOptions(dir)
	opts.EncryptionKey = key
	opts.BlockCacheSize = 1 << 20

	db, err := badger.Open(opts)
	require.NoError(t, err)
	// Closing the db.
	require.NoError(t, db.Close())

	// Opening the db again for the successful open.
	db, err = badger.Open(opts)
	require.NoError(t, err)
	// Closing so that we can open another db
	require.NoError(t, db.Close())

	// Creating another sample key.
	key2 := make([]byte, 32)
	_, err = rand.Read(key2)
	require.NoError(t, err)
	fp2, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fp2.Write(key2)
	require.NoError(t, err)
	defer fp2.Close()
	oldKeyPath = fp2.Name()
	sstDir = dir

	// Check whether we able to rotate the key with some sample key. We should get mismatch
	// error.
	require.EqualError(t, doRotate(nil, []string{}), badger.ErrEncryptionKeyMismatch.Error())

	// rotating key with proper key.
	oldKeyPath = fp.Name()
	newKeyPath = fp2.Name()
	require.NoError(t, doRotate(nil, []string{}))

	// Checking whether db opens with the new key.
	opts.EncryptionKey = key2
	db, err = badger.Open(opts)
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Checking for plain text rotation.
	oldKeyPath = newKeyPath
	newKeyPath = ""
	require.NoError(t, doRotate(nil, []string{}))
	opts.EncryptionKey = []byte{}
	db, err = badger.Open(opts)
	require.NoError(t, err)
	defer db.Close()
}

// TestRotateRewrite drives the online rotation to completion through the
// offline "rotate --rewrite" command: all data must remain readable with the
// new key, no old key generation may survive and the checkpoint must be gone.
func TestRotateRewrite(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	key := make([]byte, 32)
	_, err = rand.Read(key)
	require.NoError(t, err)
	fp, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fp.Write(key)
	require.NoError(t, err)
	defer fp.Close()

	key2 := make([]byte, 32)
	_, err = rand.Read(key2)
	require.NoError(t, err)
	fp2, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fp2.Write(key2)
	require.NoError(t, err)
	defer fp2.Close()

	// Create an encrypted database with both inline values and vlog values.
	opts := badger.DefaultOptions(dir).
		WithBlockCacheSize(1 << 20).
		WithIndexCacheSize(1 << 20).
		WithEncryptionKey(key).
		WithValueThreshold(64).
		WithValueLogFileSize(1 << 20).
		WithMemTableSize(1 << 20).
		WithBaseTableSize(128 << 10)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	for i := 0; i < 500; i++ {
		require.NoError(t, db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(fmt.Sprintf("vlog-%06d", i)), make([]byte, 2048))
		}))
	}
	for i := 0; i < 3000; i++ {
		require.NoError(t, db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(fmt.Sprintf("lsm-%06d", i)), []byte("x"))
		}))
	}
	require.NoError(t, db.Close())

	oldKeyPath = fp.Name()
	newKeyPath = fp2.Name()
	sstDir = dir
	vlogDir = dir
	rewriteFiles = true
	defer func() { rewriteFiles = false }()

	require.NoError(t, doRotate(nil, []string{}))

	// Old key no longer works; new key opens and all data is readable.
	_, err = badger.Open(opts.WithEncryptionKey(key))
	require.EqualError(t, err, badger.ErrEncryptionKeyMismatch.Error())

	db, err = badger.Open(opts.WithEncryptionKey(key2))
	require.NoError(t, err)
	for i := 0; i < 500; i++ {
		require.NoError(t, db.View(func(txn *badger.Txn) error {
			item, err := txn.Get([]byte(fmt.Sprintf("vlog-%06d", i)))
			require.NoError(t, err)
			return item.Value(func(v []byte) error {
				require.Len(t, v, 2048)
				return nil
			})
		}))
	}
	count := 0
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	}))
	require.Equal(t, 3500, count)

	// Rotation finished: status inactive, checkpoint removed, single key gen.
	st := db.KeyRotationStatus()
	require.False(t, st.Active)
	require.Equal(t, 0, st.Remaining())
	_, err = os.Stat(filepath.Join(dir, "KEYROTATION"))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, db.Close())
}

// This test shows that rotate tool can be used to enable encryption.
func TestRotatePlainTextToEncrypted(t *testing.T) {
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	// Open DB without encryption.
	opts := badger.DefaultOptions(dir)
	db, err := badger.Open(opts)
	require.NoError(t, err)

	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("foo"), []byte("bar"))
	}))

	require.NoError(t, db.Close())

	// Create an encryption key.
	key := make([]byte, 32)
	y.Check2(rand.Read(key))
	fp, err := os.CreateTemp("", "*.key")
	require.NoError(t, err)
	_, err = fp.Write(key)
	require.NoError(t, err)
	defer fp.Close()

	oldKeyPath = ""
	newKeyPath = fp.Name()
	sstDir = dir

	// Enable encryption. newKeyPath is encrypted.
	require.Nil(t, doRotate(nil, []string{}))

	// Try opening DB without the key.
	opts.BlockCacheSize = 1 << 20
	_, err = badger.Open(opts)
	require.EqualError(t, err, badger.ErrEncryptionKeyMismatch.Error())

	// Check whether db opens with the new key.
	opts.EncryptionKey = key
	db, err = badger.Open(opts)
	require.NoError(t, err)

	require.NoError(t, db.View(func(txn *badger.Txn) error {
		iopt := badger.DefaultIteratorOptions
		it := txn.NewIterator(iopt)
		defer it.Close()
		count := 0
		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		require.Equal(t, 1, count)
		return nil
	}))
	require.NoError(t, db.Close())
}
