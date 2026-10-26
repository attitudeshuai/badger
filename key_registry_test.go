/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func getRegistryTestOptions(dir string, key []byte) KeyRegistryOptions {
	return KeyRegistryOptions{
		Dir:           dir,
		EncryptionKey: key,
		ReadOnly:      false,
	}
}
func TestBuildRegistry(t *testing.T) {
	encryptionKey := make([]byte, 32)
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)

	_, err = rand.Read(encryptionKey)
	require.NoError(t, err)
	opt := getRegistryTestOptions(dir, encryptionKey)

	kr, err := OpenKeyRegistry(opt)
	require.NoError(t, err)
	dk, err := kr.LatestDataKey()
	require.NoError(t, err)
	// We're resetting the last created timestamp. So, it creates
	// new datakey.
	kr.lastCreated = 0
	dk1, err := kr.LatestDataKey()
	// We generated two key. So, checking the length.
	require.Equal(t, 2, len(kr.dataKeys))
	require.NoError(t, err)
	require.NoError(t, kr.Close())

	kr2, err := OpenKeyRegistry(opt)
	require.NoError(t, err)
	require.Equal(t, 2, len(kr2.dataKeys))
	// Asserting the correctness of the datakey after opening the registry.
	require.Equal(t, dk.Data, kr.dataKeys[dk.KeyId].Data)
	require.Equal(t, dk1.Data, kr.dataKeys[dk1.KeyId].Data)
	require.NoError(t, kr2.Close())
}

func TestRewriteRegistry(t *testing.T) {
	encryptionKey := make([]byte, 32)
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	_, err = rand.Read(encryptionKey)
	require.NoError(t, err)
	opt := getRegistryTestOptions(dir, encryptionKey)
	kr, err := OpenKeyRegistry(opt)
	require.NoError(t, err)
	_, err = kr.LatestDataKey()
	require.NoError(t, err)
	// We're resetting the last created timestamp. So, it creates
	// new datakey.
	kr.lastCreated = 0
	_, err = kr.LatestDataKey()
	require.NoError(t, err)
	require.NoError(t, kr.Close())
	delete(kr.dataKeys, 1)
	require.NoError(t, WriteKeyRegistry(kr, opt))
	kr2, err := OpenKeyRegistry(opt)
	require.NoError(t, err)
	require.Equal(t, 1, len(kr2.dataKeys))
	require.NoError(t, kr2.Close())
}

func TestMismatch(t *testing.T) {
	encryptionKey := make([]byte, 32)
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	_, err = rand.Read(encryptionKey)
	require.NoError(t, err)
	opt := getRegistryTestOptions(dir, encryptionKey)
	kr, err := OpenKeyRegistry(opt)
	require.NoError(t, err)
	require.NoError(t, kr.Close())
	// Opening with the same key and asserting.
	kr, err = OpenKeyRegistry(opt)
	require.NoError(t, err)
	require.NoError(t, kr.Close())
	// Opening with the invalid key and asserting.
	encryptionKey = make([]byte, 32)
	_, err = rand.Read(encryptionKey)
	require.NoError(t, err)
	opt.EncryptionKey = encryptionKey
	_, err = OpenKeyRegistry(opt)
	require.Error(t, err)
	require.EqualError(t, err, ErrEncryptionKeyMismatch.Error())
}

func TestEncryptionAndDecryption(t *testing.T) {
	encryptionKey := make([]byte, 32)
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	_, err = rand.Read(encryptionKey)
	require.NoError(t, err)
	opt := getRegistryTestOptions(dir, encryptionKey)
	kr, err := OpenKeyRegistry(opt)
	require.NoError(t, err)
	dk, err := kr.LatestDataKey()
	require.NoError(t, err)
	require.NoError(t, kr.Close())
	// Checking the correctness of the datakey after closing and
	// opening the key registry.
	kr, err = OpenKeyRegistry(opt)
	require.NoError(t, err)
	dk1, err := kr.DataKey(dk.GetKeyId())
	require.NoError(t, err)
	require.Equal(t, dk.Data, dk1.Data)
	require.NoError(t, kr.Close())
}

func TestKeyRegistryInMemory(t *testing.T) {
	encryptionKey := make([]byte, 32)
	_, err := rand.Read(encryptionKey)
	require.NoError(t, err)

	opt := getRegistryTestOptions("", encryptionKey)
	opt.InMemory = true

	kr, err := OpenKeyRegistry(opt)
	require.NoError(t, err)
	_, err = kr.LatestDataKey()
	require.NoError(t, err)
	// We're resetting the last created timestamp. So, it creates
	// new datakey.
	kr.lastCreated = 0
	_, err = kr.LatestDataKey()
	// We generated two key. So, checking the length.
	require.Equal(t, 2, len(kr.dataKeys))
	require.NoError(t, err)
	require.NoError(t, kr.Close())
}

// TestRegistryRotateMasterKey covers the online registry re-wrap: a new data
// key is generated and persisted under the new master key, old data keys stay
// readable, pinning forces LatestDataKey to the target generation, and
// PruneDataKeys retires old generations.
func TestRegistryRotateMasterKey(t *testing.T) {
	encryptionKey := make([]byte, 32)
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	_, err = rand.Read(encryptionKey)
	require.NoError(t, err)
	opt := getRegistryTestOptions(dir, encryptionKey)

	kr, err := OpenKeyRegistry(opt)
	require.NoError(t, err)
	dk1, err := kr.LatestDataKey()
	require.NoError(t, err)
	require.Equal(t, uint64(1), dk1.KeyId)

	newKey := make([]byte, 32)
	_, err = rand.Read(newKey)
	require.NoError(t, err)

	// Rotating to a bad length must fail and leave the registry usable.
	_, err = kr.RotateMasterKey([]byte("short"))
	require.Error(t, err)

	target, err := kr.RotateMasterKey(newKey)
	require.NoError(t, err)
	require.Equal(t, uint64(2), target.KeyId)
	// Both data keys are present and readable in memory.
	require.Len(t, kr.dataKeys, 2)

	// The old master key must no longer open the registry; the new one opens
	// it and shows both data keys.
	require.NoError(t, kr.Close())
	oldOpt := opt
	_, err = OpenKeyRegistry(oldOpt)
	require.EqualError(t, err, ErrEncryptionKeyMismatch.Error())

	newOpt := opt
	newOpt.EncryptionKey = newKey
	kr2, err := OpenKeyRegistry(newOpt)
	require.NoError(t, err)
	require.Len(t, kr2.dataKeys, 2)
	dk1b, err := kr2.DataKey(dk1.KeyId)
	require.NoError(t, err)
	require.Equal(t, dk1.Data, dk1b.Data)
	targetB, err := kr2.DataKey(target.KeyId)
	require.NoError(t, err)
	require.Equal(t, target.Data, targetB.Data)

	// Pinning ignores the rotation duration: even with an ancient timestamp,
	// LatestDataKey returns the pinned target generation.
	require.NoError(t, kr2.PinDataKey(target.KeyId))
	kr2.lastCreated = 0
	got, err := kr2.LatestDataKey()
	require.NoError(t, err)
	require.Equal(t, target.KeyId, got.KeyId)
	pinnedID, pinned := kr2.PinnedKeyID()
	require.True(t, pinned)
	require.Equal(t, target.KeyId, pinnedID)

	// Pinning an unknown generation fails.
	require.Error(t, kr2.PinDataKey(9999))

	// Prune keeps only the target and persists the trimmed registry.
	require.NoError(t, kr2.PruneDataKeys(target.KeyId))
	kr2.UnpinDataKey()
	require.NoError(t, kr2.Close())

	kr3, err := OpenKeyRegistry(newOpt)
	require.NoError(t, err)
	require.Len(t, kr3.dataKeys, 1)
	_, err = kr3.DataKey(dk1.KeyId)
	require.Error(t, err)
	require.NoError(t, kr3.Close())
}

// TestRegistryRotateToPlainText verifies a registry can be re-wrapped without
// a master key (rotation to plain text) and reopened without one.
func TestRegistryRotateToPlainText(t *testing.T) {
	encryptionKey := make([]byte, 32)
	dir, err := os.MkdirTemp("", "badger-test")
	require.NoError(t, err)
	defer removeDir(dir)
	_, err = rand.Read(encryptionKey)
	require.NoError(t, err)
	opt := getRegistryTestOptions(dir, encryptionKey)

	kr, err := OpenKeyRegistry(opt)
	require.NoError(t, err)
	_, err = kr.LatestDataKey()
	require.NoError(t, err)
	target, err := kr.RotateMasterKey(nil)
	require.NoError(t, err)
	require.Nil(t, target)
	require.NoError(t, kr.Close())

	plainOpt := opt
	plainOpt.EncryptionKey = nil
	kr2, err := OpenKeyRegistry(plainOpt)
	require.NoError(t, err)
	// Existing data keys survive the wrap change so old files stay readable.
	require.Len(t, kr2.dataKeys, 1)
	got, err := kr2.LatestDataKey()
	require.NoError(t, err)
	require.Nil(t, got) // no master key means no new data keys
	require.NoError(t, kr2.Close())
}
