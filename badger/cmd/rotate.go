/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"context"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/dgraph-io/badger/v4"
)

var oldKeyPath string
var newKeyPath string
var rewriteFiles bool

var rotateCmd = &cobra.Command{
	Use:   "rotate",
	Short: "Rotate encryption key.",
	Long: `Rotate will rotate the old key with new encryption key.

By default only the key registry is re-wrapped with the new master key; data
files stay encrypted with their existing data keys (fast, but the database
must be offline).

With --rewrite, the database is opened with the old key and the online key
rotation is driven to completion: every existing SSTable and value log file is
re-encrypted under a fresh data key generation, old key generations are
retired, and the new master key is the only key required afterwards. The same
code path as an online rotation is used, so files produced by compaction,
value log GC and this command all end up under the same key generation.
`,
	RunE: doRotate,
}

func init() {
	RootCmd.AddCommand(rotateCmd)
	rotateCmd.Flags().StringVarP(&oldKeyPath, "old-key-path", "o",
		"", "Path of the old key")
	rotateCmd.Flags().StringVarP(&newKeyPath, "new-key-path", "n",
		"", "Path of the new key")
	rotateCmd.Flags().BoolVar(&rewriteFiles, "rewrite", false,
		"Re-encrypt every existing SSTable and value log file under the new key, "+
			"driving the online rotation to completion.")
}

func doRotate(cmd *cobra.Command, args []string) error {
	oldKey, err := getKey(oldKeyPath)
	if err != nil {
		return err
	}
	newKey, err := getKey(newKeyPath)
	if err != nil {
		return err
	}
	if rewriteFiles {
		return doRotateRewrite(oldKey, newKey)
	}
	return doRotateRegistry(oldKey, newKey)
}

// doRotateRegistry keeps the historical behavior: open the registry read-only
// with the old master key and atomically rewrite it wrapped with the new master
// key. Existing data keys are preserved, so existing files stay readable.
func doRotateRegistry(oldKey, newKey []byte) error {
	opt := badger.KeyRegistryOptions{
		Dir:                           sstDir,
		ReadOnly:                      true,
		EncryptionKey:                 oldKey,
		EncryptionKeyRotationDuration: 10 * 24 * time.Hour,
	}
	kr, err := badger.OpenKeyRegistry(opt)
	if err != nil {
		return err
	}
	opt.EncryptionKey = newKey
	return badger.WriteKeyRegistry(kr, opt)
}

// doRotateRewrite opens the database with the old master key and runs the
// online key rotation to completion. It is the offline-ops consumer of the
// rotation machinery: all files it produces land under the same (pinned)
// target data key generation as ordinary writes and background compactions.
func doRotateRewrite(oldKey, newKey []byte) error {
	// Encrypted tables require the block/index caches to open.
	opt := badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithBlockCacheSize(100 << 20).
		WithIndexCacheSize(200 << 20).
		WithEncryptionKey(oldKey)
	db, err := badger.Open(opt)
	if err != nil {
		return err
	}

	if err := db.StartKeyRotation(newKey); err != nil {
		_ = db.Close()
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Surface progress while the offline command runs.
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				st := db.KeyRotationStatus()
				if !st.Active {
					continue
				}
				os.Stdout.WriteString(
					"key rotation: sst " + strconv.Itoa(st.SSTTotal-st.SSTRemaining) + "/" + strconv.Itoa(st.SSTTotal) +
						", vlog " + strconv.Itoa(st.VlogTotal-st.VlogRemaining) + "/" + strconv.Itoa(st.VlogTotal) +
						", wal " + strconv.Itoa(st.WALTotal-st.WALRemaining) + "/" + strconv.Itoa(st.WALTotal) + "\n")
			}
		}
	}()

	waitErr := db.WaitKeyRotation(ctx)
	cancel()
	closeErr := db.Close()
	if waitErr != nil {
		return waitErr
	}
	return closeErr
}

func getKey(path string) ([]byte, error) {
	if path == "" {
		// Empty bytes for plain text to encryption(vice versa).
		return []byte{}, nil
	}
	fp, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(fp)
}
