package transport

import (
	"context"
	"errors"
	"lamina/internal/engine"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

func (s *Server) checkpoint(w http.ResponseWriter, r *http.Request) {
	seq, err := s.db.Checkpoint()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, map[string]uint64{"sequence": seq})
}
func (s *Server) vacuum(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Horizon uint64 `json:"horizon"`
	}
	if !decode(w, r, &in) {
		return
	}
	out, err := s.db.Vacuum(in.Horizon)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) backup(w http.ResponseWriter, r *http.Request) {
	exp, err := s.db.StartBackup()
	if err != nil {
		respondError(w, err)
		return
	}
	// Every failure path after this point releases the frozen spool and the
	// export slot, including client disconnects and write deadlines.
	defer exp.Close()
	capture, encode := exp.Timings()
	h := w.Header()
	h.Set("Content-Disposition", `attachment; filename="lamina-backup.lmi"`)
	h.Set("Content-Length", strconv.FormatInt(exp.Size, 10))
	h.Set("X-Lamina-Sequence", strconv.FormatUint(exp.Sequence, 10))
	h.Set("X-Lamina-Horizon", strconv.FormatUint(exp.Horizon, 10))
	h.Set("X-Lamina-SHA256", exp.Digest)
	h.Set("ETag", `"`+exp.Digest+`"`)
	h.Set("X-Lamina-Capture-Ms", strconv.FormatInt(capture.Milliseconds(), 10))
	h.Set("X-Lamina-Encode-Ms", strconv.FormatInt(encode.Milliseconds(), 10))

	ctl := http.NewResponseController(w)
	// Override the server-wide WriteTimeout for this long-lived download: the
	// per-chunk stall deadline and overall budget below govern this response.
	_ = ctl.SetWriteDeadline(time.Now().Add(s.backupBudget.Overall))
	w.WriteHeader(200)

	started := time.Now()
	n, err := exp.WriteTo(w, ctl, s.backupBudget.Stall, s.backupBudget.Overall)
	if err != nil {
		exp.Abort()
		if errors.Is(r.Context().Err(), context.Canceled) {
			slog.Warn("backup download abandoned by client", "sequence", exp.Sequence, "delivered_bytes", n, "elapsed", time.Since(started))
		} else {
			slog.Warn("backup download failed", "sequence", exp.Sequence, "delivered_bytes", n, "elapsed", time.Since(started), "error", err)
		}
		return
	}
	if n != exp.Size {
		exp.Abort()
		slog.Warn("backup download short write", "sequence", exp.Sequence, "expected", exp.Size, "delivered", n)
		return
	}
	exp.Succeed(time.Since(started), n)
}

// backupHead rejects HEAD without creating a spool: a frozen export is a
// bounded resource and metadata probing must not consume the capture budget.
func (s *Server) backupHead(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Allow", "GET")
	write(w, http.StatusMethodNotAllowed, map[string]any{"error": map[string]string{"code": "invalid", "message": "HEAD is not supported for backup; issue GET to create a frozen export"}})
}
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.Stats()
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
func (s *Server) changes(w http.ResponseWriter, r *http.Request) {
	after := uint64(0)
	limit := 100
	var err error
	if value := r.URL.Query().Get("after"); value != "" {
		after, err = strconv.ParseUint(value, 10, 64)
		if err != nil {
			respondError(w, &engine.Error{Code: "invalid", Message: "invalid after"})
			return
		}
	}
	if value := r.URL.Query().Get("limit"); value != "" {
		limit, err = strconv.Atoi(value)
		if err != nil {
			respondError(w, &engine.Error{Code: "invalid", Message: "invalid limit"})
			return
		}
	}
	out, err := s.db.Changes(after, limit)
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
