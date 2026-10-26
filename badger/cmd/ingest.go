/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package cmd

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/dgraph-io/badger/v4"
)

var ingestCmdOpts struct {
	list    bool
	discard string
}

// ingestCmd inspects and resolves bulk-ingest sessions.
var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "Inspect or discard bulk-ingest sessions.",
	Long: `Lists bulk-ingest sessions recorded in the database and explicitly discards
unfinished ones. Session state is derived from the same on-disk journals that
database open and the info command use, so all consumers report identical
status.

With --discard <id>, all staged tables belonging to an unfinished session are
removed and the session journal is deleted. Already committed sessions cannot
be discarded.`,
	RunE: handleIngest,
}

func init() {
	RootCmd.AddCommand(ingestCmd)
	ingestCmd.Flags().BoolVar(&ingestCmdOpts.list, "list", true,
		"List all bulk-ingest sessions (pending and committed).")
	ingestCmd.Flags().StringVar(&ingestCmdOpts.discard, "discard", "",
		"Discard the unfinished ingest session with the given ID instead of listing sessions.")
}

func handleIngest(cmd *cobra.Command, args []string) error {
	if ingestCmdOpts.discard != "" {
		return discardIngest(ingestCmdOpts.discard)
	}
	return listIngest()
}

func baseIngestOptions() badger.Options {
	return badger.DefaultOptions(sstDir).
		WithValueDir(vlogDir).
		WithEncryptionKey([]byte(opt.encryptionKey)).
		WithExternalMagic(opt.externalMagicVersion)
}

func listIngest() error {
	// Read-only inspection: does not open (or lock) the database.
	infos, err := badger.ReadIngestSessions(baseIngestOptions())
	if err != nil {
		return err
	}
	printIngestSessionList(infos)
	return nil
}

func discardIngest(id string) error {
	bopt := baseIngestOptions().
		WithBlockCacheSize(100 << 20).
		WithIndexCacheSize(200 << 20)
	db, err := badger.Open(bopt)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.DiscardIngestSession(id); err != nil {
		return err
	}
	fmt.Printf("Discarded ingest session %q.\n", id)
	return nil
}

// printIngestSessionList renders sessions for both the `ingest` command and
// `badger info`. It returns the number of unfinished sessions.
func printIngestSessionList(infos []badger.IngestSessionInfo) int {
	fmt.Println()
	fmt.Println("INGEST SESSIONS " +
		"[ID, Mode, Phase, Target Level] [Key Range, Tables, Started]")
	if len(infos) == 0 {
		fmt.Println("(none)")
		return 0
	}
	pending := 0
	for _, s := range infos {
		if s.Phase == badger.IngestStaging || s.Phase == badger.IngestCommitting {
			pending++
		}
		fmt.Printf("[%s, %s, %s, L%d] range=%X..%X tables=%d started=%s\n",
			s.ID, s.Mode, s.Phase, s.TargetLevel, s.Range.Left, s.Range.Right,
			s.NumTables, fmtTime(s.StartedAt))
		if s.Phase == badger.IngestCommitted {
			fmt.Printf("    committed=%s final-tables=%d\n",
				fmtTime(s.FinishedAt), len(s.FinalTableIDs))
		}
	}
	fmt.Printf("Unfinished ingest sessions: %d\n", pending)
	return pending
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Format(time.RFC3339)
}
