package engine

import (
	"sort"
	"time"
)

type Stats struct {
	Sequence       uint64         `json:"sequence"`
	Horizon        uint64         `json:"horizon"`
	Active         []Transaction  `json:"active"`
	Keys           int            `json:"keys"`
	Versions       int            `json:"versions"`
	IndexEntries   map[string]int `json:"index_entries"`
	JournalRecords int            `json:"journal_records"`
	HistoryBase    uint64         `json:"history_base"`
	RecoveredTail  int64          `json:"recovered_tail_bytes"`
	Commits        uint64         `json:"commits_since_open"`
	Conflicts      uint64         `json:"conflicts_since_open"`
	Queries        uint64         `json:"queries_since_open"`
	Backups        BackupStats    `json:"backups"`
}

// BackupStats exposes the observable trade-off between capture cost, service
// latency and export throughput. Capture and encode run per export; delivery
// aggregates only fully completed downloads.
type BackupStats struct {
	ConcurrencyBudget    int           `json:"concurrency_budget"`
	ActiveExports        int           `json:"active_exports"`
	Started              uint64        `json:"started"`
	Spooled              uint64        `json:"spooled"`
	Completed            uint64        `json:"completed"`
	Aborted              uint64        `json:"aborted"`
	DeliveredBytes       int64         `json:"delivered_bytes"`
	CaptureElapsedTotal  time.Duration `json:"capture_elapsed_total"`
	EncodeElapsedTotal   time.Duration `json:"encode_elapsed_total"`
	DeliveryElapsedTotal time.Duration `json:"delivery_elapsed_total"`
}

func (d *DB) Stats() (Stats, error) {
	d.mu.RLock()
	if err := d.usable(); err != nil {
		d.mu.RUnlock()
		return Stats{}, err
	}
	keys, versions := d.store.Count()
	s := Stats{Sequence: d.sequence, Horizon: d.horizon, Keys: keys, Versions: versions, Active: []Transaction{}, IndexEntries: map[string]int{}, JournalRecords: len(d.wal.Records), HistoryBase: d.wal.Base, RecoveredTail: d.wal.RecoveredTail, Commits: d.commits, Conflicts: d.conflicts, Queries: d.queries}
	for _, tx := range d.transactions {
		copy := *tx
		copy.writes = nil
		s.Active = append(s.Active, copy)
	}
	sort.Slice(s.Active, func(i, j int) bool { return s.Active[i].ID < s.Active[j].ID })
	for name, t := range d.indexes.Trees {
		s.IndexEntries[name] = t.Size()
	}
	d.mu.RUnlock()

	d.exportCounterMu.Lock()
	s.Backups = BackupStats{
		ConcurrencyBudget:    cap(d.exportSlots),
		ActiveExports:        len(d.exportSlots),
		Started:              d.backupsStarted,
		Spooled:              d.backupsSpooled,
		Completed:            d.backupsCompleted,
		Aborted:              d.backupsAborted,
		DeliveredBytes:       d.backupDeliveredBytes,
		CaptureElapsedTotal:  d.backupCaptureElapsed,
		EncodeElapsedTotal:   d.backupEncodeElapsed,
		DeliveryElapsedTotal: d.backupDeliveryElapsed,
	}
	d.exportCounterMu.Unlock()
	return s, nil
}
