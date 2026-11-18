package main

import (
	"context"
	"flag"
	"fmt"
	"lamina/internal/engine"
	"lamina/internal/snapshot"
	"lamina/internal/transport"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("lamina stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	dir := flag.String("data", "var/lamina", "data directory exclusively owned by this process")
	addr := flag.String("listen", "127.0.0.1:7386", "HTTP bind address")
	restore := flag.String("restore", "", "validated backup to install into a new data directory before serving")
	backupConcurrency := flag.Int("backup-concurrency", engine.DefaultMaxConcurrentExports, "maximum concurrent frozen backup exports (each may spool up to 128MiB)")
	backupStall := flag.Duration("backup-stall", 60*time.Second, "max idle time between backup chunks sent to a slow client")
	backupOverall := flag.Duration("backup-overall", 30*time.Minute, "max total duration of one backup download")
	flag.Parse()
	if *restore != "" {
		info, err := os.Stat(*restore)
		if err != nil {
			return err
		}
		if info.Size() > snapshot.MaxImage {
			return fmt.Errorf("backup exceeds limit")
		}
		b, err := os.ReadFile(*restore)
		if err != nil {
			return err
		}
		if err = engine.Restore(*dir, b); err != nil {
			return err
		}
	}
	db, err := engine.Open(*dir)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetBackupConcurrency(*backupConcurrency)
	server := &http.Server{Addr: *addr, Handler: transport.NewWithConfig(db, transport.Config{BackupStall: *backupStall, BackupOverall: *backupOverall}), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 45 * time.Second, MaxHeaderBytes: 32 << 10}
	stopped := make(chan os.Signal, 1)
	signal.Notify(stopped, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(stopped)
	finished := make(chan error, 1)
	go func() { slog.Info("lamina ready", "address", *addr); finished <- server.ListenAndServe() }()
	select {
	case err = <-finished:
		if err != http.ErrServerClosed {
			return err
		}
	case <-stopped:
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err = server.Shutdown(ctx); err != nil {
			server.Close()
			return err
		}
	}
	return nil
}
