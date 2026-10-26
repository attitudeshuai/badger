/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/pb"
	"github.com/dgraph-io/ristretto/v2/z"
)

func setupIngestCmdDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "badger-ingest-cmd")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	// Committed session.
	db, err := badger.Open(badger.DefaultOptions(dir).WithLoggingLevel(badger.WARNING))
	require.NoError(t, err)
	sw := db.NewStreamWriter()
	require.NoError(t, sw.Begin(badger.IngestFull, badger.IngestOptions{SessionID: "cmd-done"}))
	buf := z.NewBuffer(1<<20, "test")
	badger.KVToBuffer(&pb.KV{Key: []byte("a"), Value: []byte("1"), Version: 1}, buf)
	require.NoError(t, sw.Write(buf))
	require.NoError(t, buf.Release())
	require.NoError(t, sw.Flush())
	require.NoError(t, db.Close())

	// Unfinished incremental session (closed mid-staging).
	db, err = badger.Open(badger.DefaultOptions(dir).WithLoggingLevel(badger.WARNING))
	require.NoError(t, err)
	sw = db.NewStreamWriter()
	require.NoError(t, sw.Begin(badger.IngestIncremental, badger.IngestOptions{SessionID: "cmd-pending"}))
	buf = z.NewBuffer(1<<20, "test")
	badger.KVToBuffer(&pb.KV{Key: []byte("z"), Value: []byte("2"), Version: 2}, buf)
	require.NoError(t, sw.Write(buf))
	require.NoError(t, buf.Release())
	require.NoError(t, db.Close())

	return dir
}

func TestIngestCmdList(t *testing.T) {
	dir := setupIngestCmdDir(t)
	sstDir, vlogDir = dir, dir
	opt.encryptionKey = ""

	infos, err := badger.ReadIngestSessions(baseIngestOptions())
	require.NoError(t, err)
	byID := map[string]badger.IngestSessionInfo{}
	for _, info := range infos {
		byID[info.ID] = info
	}
	require.Equal(t, badger.IngestCommitted, byID["cmd-done"].Phase)
	require.True(t, byID["cmd-pending"].Phase.Pending())

	// Listing requires no writable open and is the exact code path the command
	// uses.
	require.NoError(t, listIngest())
}

func TestIngestCmdDiscard(t *testing.T) {
	dir := setupIngestCmdDir(t)
	sstDir, vlogDir = dir, dir
	opt.encryptionKey = ""

	require.ErrorIs(t, discardIngest("cmd-done"), badger.ErrIngestAlreadyCommitted)
	require.ErrorIs(t, discardIngest("missing"), badger.ErrIngestSessionNotFound)
	require.NoError(t, discardIngest("cmd-pending"))

	infos, err := badger.ReadIngestSessions(baseIngestOptions())
	require.NoError(t, err)
	for _, info := range infos {
		require.NotEqual(t, "cmd-pending", info.ID)
	}

	// Data committed before the discarded session is intact and the DB opens.
	db, err := badger.Open(badger.DefaultOptions(dir).WithLoggingLevel(badger.WARNING))
	require.NoError(t, err)
	defer db.Close()
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, gerr := txn.Get([]byte("a"))
		require.NoError(t, gerr)
		_, zerr := txn.Get([]byte("z"))
		require.ErrorIs(t, zerr, badger.ErrKeyNotFound)
		return nil
	}))
}
