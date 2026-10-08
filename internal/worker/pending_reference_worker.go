package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	defaultPendingReferenceInterval  = 500 * time.Millisecond
	defaultPendingReferenceBatchSize = 50
)

type PendingReferenceProcessor interface {
	ProcessPendingReferences(ctx context.Context, limit int) (int, error)
}

type PendingReferenceMetrics interface {
	ObservePendingReferences(outcome string, count int)
}

type PendingReferenceWorker struct {
	processor PendingReferenceProcessor
	interval  time.Duration
	batchSize int
	logger    *slog.Logger
	metrics   PendingReferenceMetrics

	mu      sync.Mutex
	cancel  context.CancelFunc
	running bool
	wg      sync.WaitGroup
}

func NewPendingReferenceWorker(
	processor PendingReferenceProcessor,
	metrics ...PendingReferenceMetrics,
) *PendingReferenceWorker {
	worker := &PendingReferenceWorker{
		processor: processor,
		interval:  defaultPendingReferenceInterval,
		batchSize: defaultPendingReferenceBatchSize,
		logger:    slog.Default(),
	}
	if len(metrics) > 0 {
		worker.metrics = metrics[0]
	}
	return worker
}

func (w *PendingReferenceWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return
	}

	workerCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.running = true
	w.wg.Add(1)
	w.mu.Unlock()

	go w.run(workerCtx)
}

func (w *PendingReferenceWorker) Stop() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	cancel := w.cancel
	w.mu.Unlock()

	cancel()
	w.wg.Wait()
}

func (w *PendingReferenceWorker) run(ctx context.Context) {
	defer func() {
		w.mu.Lock()
		w.running = false
		w.cancel = nil
		w.mu.Unlock()
		w.wg.Done()
	}()

	w.processCycle(ctx)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.processCycle(ctx)
		}
	}
}

func (w *PendingReferenceWorker) processCycle(ctx context.Context) {
	processed, err := w.processor.ProcessPendingReferences(ctx, w.batchSize)
	if err != nil {
		if w.metrics != nil {
			w.metrics.ObservePendingReferences("error", 1)
		}
		if ctx.Err() == nil {
			w.logger.Error("pending reference processing cycle failed", "error", err)
		}
		return
	}
	if processed > 0 {
		if w.metrics != nil {
			w.metrics.ObservePendingReferences("resolved", processed)
		}
		w.logger.Info("pending references processed", "count", processed)
		return
	}
	if w.metrics != nil {
		w.metrics.ObservePendingReferences("idle", 1)
	}
}
