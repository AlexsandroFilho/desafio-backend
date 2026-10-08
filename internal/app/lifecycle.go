package app

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"

	"alexsandro-filho/desafio-backend/internal/worker"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

func registerLifecycle(
	lifecycle fx.Lifecycle,
	server *http.Server,
	outboxWorker *worker.OutboxWorker,
	pendingReferenceWorker *worker.PendingReferenceWorker,
	sqsConsumer *worker.SQSConsumer,
	database *pgxpool.Pool,
	logger *slog.Logger,
) {
	var cancelRuntime context.CancelFunc

	lifecycle.Append(fx.Hook{
		OnStart: func(context.Context) error {
			listener, err := net.Listen("tcp", server.Addr)
			if err != nil {
				return err
			}

			runtimeContext, cancel := context.WithCancel(context.Background())
			cancelRuntime = cancel

			go func() {
				if err := server.Serve(listener); err != nil &&
					!errors.Is(err, http.ErrServerClosed) {
					logger.Error("HTTP server stopped unexpectedly", "error", err)
				}
			}()

			outboxWorker.Start(runtimeContext)
			pendingReferenceWorker.Start(runtimeContext)
			sqsConsumer.Start(runtimeContext)
			logger.Info("application started", "http_address", server.Addr)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			if cancelRuntime != nil {
				cancelRuntime()
			}

			sqsConsumer.Stop()
			pendingReferenceWorker.Stop()
			outboxWorker.Stop()

			shutdownErr := server.Shutdown(ctx)
			database.Close()
			logger.Info("application stopped")
			return shutdownErr
		},
	})
}
