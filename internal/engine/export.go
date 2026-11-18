package engine

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"lamina/internal/snapshot"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

// DefaultMaxConcurrentExports bounds how many frozen images and spool files
// may exist at once. Each export can occupy up to snapshot.MaxImage bytes on
// disk, so this is an explicit total spool budget, not just a request cap.
const DefaultMaxConcurrentExports = 4

const exportDirMode = 0700
const exportFileMode = 0600

// Export is one frozen database image spooled to disk and awaiting delivery.
// A single download always serves this immutable file: later commits and
// vacuums touch the live store only, never the captured clone behind Export.
type Export struct {
	Sequence uint64
	Horizon  uint64
	Digest   string
	Size     int64

	path     string
	spool    *os.File
	captured time.Duration
	encoded  time.Duration

	closeOnce sync.Once
	closed    bool
	mu        sync.Mutex
	release   func()
	record    func(completed bool, elapsed time.Duration, bytes int64)
}

// sweepSpool removes files left by an earlier process that could not run its
// deferred cleanup. It is only safe at startup: the directory lock guarantees
// no other export exists in this process yet.
func (d *DB) sweepSpool() error {
	dir := filepath.Join(d.dir, "exports")
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		_ = os.Remove(filepath.Join(dir, entry.Name()))
	}
	return nil
}

// prepareSpool ensures the export directory exists.
func (d *DB) prepareSpool() error {
	return os.MkdirAll(filepath.Join(d.dir, "exports"), exportDirMode)
}

// SetBackupConcurrency reconfigures the export budget before serving traffic.
// A non-positive value keeps the current budget.
func (d *DB) SetBackupConcurrency(n int) {
	if n > 0 {
		d.exportSlots = make(chan struct{}, n)
	}
}

// StartBackup captures a frozen image under the read lock, then releases the
// lock and encodes it into a bounded temporary file. Ordinary transactions
// continue while encoding and delivery run. The returned Export must be
// closed exactly once; Close deletes the spool file and frees the slot.
func (d *DB) StartBackup() (exp *Export, err error) {
	select {
	case d.exportSlots <- struct{}{}:
	default:
		return nil, fault("capacity", "backup concurrency budget reached")
	}
	committed := false
	releaseSlot := func() { <-d.exportSlots }
	defer func() {
		if !committed {
			releaseSlot()
		}
	}()

	// Account the attempt before any failure point so the stats invariant
	// started == completed + aborted always holds.
	d.exportCounterMu.Lock()
	d.backupsStarted++
	d.exportCounterMu.Unlock()

	captureStart := time.Now()
	d.mu.RLock()
	if err = d.usable(); err != nil {
		d.mu.RUnlock()
		d.countExportAbort()
		return nil, err
	}
	image := d.image()
	sequence, horizon := d.sequence, d.horizon
	d.mu.RUnlock()
	captured := time.Since(captureStart)

	if err = d.prepareSpool(); err != nil {
		d.countExportAbort()
		return nil, fault("unavailable", "backup spool unavailable: %s", err)
	}
	var suffix [8]byte
	if _, err = rand.Read(suffix[:]); err != nil {
		d.countExportAbort()
		return nil, err
	}
	path := filepath.Join(d.dir, "exports", ".lamina-export-"+hex.EncodeToString(suffix[:]))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, exportFileMode)
	if err != nil {
		d.countExportAbort()
		return nil, mapSpoolError(err)
	}
	exp = &Export{
		Sequence: sequence,
		Horizon:  horizon,
		path:     path,
		spool:    f,
		captured: captured,
		release:  releaseSlot,
	}
	cleanup := true
	defer func() {
		if cleanup {
			exp.remove()
		}
	}()

	encodeStart := time.Now()
	digest, size, encErr := snapshot.EncodeTo(f, image)
	exp.encoded = time.Since(encodeStart)
	if encErr != nil {
		if errors.Is(encErr, snapshot.ErrImageTooLarge) {
			err = fault("capacity", "backup exceeds 128MiB")
		} else {
			err = mapSpoolError(encErr)
		}
		d.countExportAbort()
		return nil, err
	}
	if err = f.Sync(); err != nil {
		d.countExportAbort()
		return nil, mapSpoolError(err)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		d.countExportAbort()
		return nil, fault("unavailable", "backup spool seek failed: %s", err)
	}
	exp.Digest = digest
	exp.Size = size
	exp.record = func(completed bool, elapsed time.Duration, bytes int64) {
		d.exportCounterMu.Lock()
		defer d.exportCounterMu.Unlock()
		if completed {
			d.backupsCompleted++
			d.backupDeliveredBytes += bytes
			d.backupDeliveryElapsed += elapsed
			d.backupCaptureElapsed += exp.captured
			d.backupEncodeElapsed += exp.encoded
		} else {
			d.backupsAborted++
		}
	}

	d.exportCounterMu.Lock()
	d.backupsSpooled++
	d.exportCounterMu.Unlock()
	committed = true
	cleanup = false
	return exp, nil
}

// WriteDeadliner is the subset of http.ResponseController used during
// delivery; transport supplies the concrete controller so engine stays free
// of net/http.
type WriteDeadliner interface {
	SetWriteDeadline(time.Time) error
}

// WriteTo streams the frozen spool file to dst in bounded chunks, refreshing
// the write deadline before every chunk so a slow but progressing client is
// allowed while a stalled one is cut off. The overall deadline caps the total
// transfer. The spool is read-only here; the HTTP layer gives every request
// its own Export instance, so bytes can never mix across downloads.
func (e *Export) WriteTo(dst io.Writer, ctl WriteDeadliner, stall, overall time.Duration) (int64, error) {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return 0, errors.New("export already released")
	}
	spool := e.spool
	e.mu.Unlock()
	buf := make([]byte, 64<<10)
	overallDeadline := time.Now().Add(overall)
	var total int64
	for {
		n, readErr := spool.Read(buf)
		if n > 0 {
			if ctl != nil {
				_ = ctl.SetWriteDeadline(time.Now().Add(stall))
			}
			w, writeErr := dst.Write(buf[:n])
			total += int64(w)
			if writeErr != nil {
				return total, writeErr
			}
			if ctl != nil {
				_ = ctl.SetWriteDeadline(overallDeadline)
			}
		}
		if readErr == io.EOF {
			return total, nil
		}
		if readErr != nil {
			return total, readErr
		}
	}
}

// Succeed records a fully delivered export.
func (e *Export) Succeed(elapsed time.Duration, bytes int64) {
	if e.record == nil {
		return
	}
	e.record(true, elapsed, bytes)
}

// Abort records an export whose delivery did not finish (client disconnect,
// write deadline, transport failure).
func (e *Export) Abort() {
	if e.record != nil {
		e.record(false, 0, 0)
	}
}

// Close releases the spool file and the concurrency slot. It is idempotent.
func (e *Export) Close() {
	e.closeOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		spool := e.spool
		e.spool = nil
		e.mu.Unlock()
		if spool != nil {
			_ = spool.Close()
		}
		_ = os.Remove(e.path)
		if e.release != nil {
			e.release()
		}
	})
}

func (e *Export) remove() {
	if e.spool != nil {
		_ = e.spool.Close()
	}
	_ = os.Remove(e.path)
}

// Timings returns the frozen-state capture and encode cost of this export for
// the response headers that make the capture/latency/throughput trade-off
// observable.
func (e *Export) Timings() (capture, encode time.Duration) {
	return e.captured, e.encoded
}

func mapSpoolError(err error) error {
	if errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT) {
		return fault("capacity", "backup spool out of space: %s", err)
	}
	return fault("unavailable", "backup spool failed: %s", err)
}

func (d *DB) countExportAbort() {
	d.exportCounterMu.Lock()
	d.backupsAborted++
	d.exportCounterMu.Unlock()
}
