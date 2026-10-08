package app

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"alexsandro-filho/desafio-backend/internal/http/handler"
	"alexsandro-filho/desafio-backend/internal/http/middleware"
	"alexsandro-filho/desafio-backend/internal/observability"
	"alexsandro-filho/desafio-backend/internal/repository"
	"alexsandro-filho/desafio-backend/internal/usecase"
	"alexsandro-filho/desafio-backend/internal/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
)

var Module = fx.Module(
	"wager-application",
	fx.Provide(
		LoadConfig,
		provideLogger,
		observability.NewMetrics,
		providePostgresPool,
		provideSQSClients,
		provideAuthMiddleware,
		provideWalletRepository,
		provideTransactionRepository,
		provideLedgerRepository,
		provideInboxOutboxRepository,
		provideWagerServices,
		provideWalletApplication,
		handler.NewWalletHandler,
		handler.NewWagerHandler,
		provideHealthHandler,
		provideHTTPServer,
		provideOutboxWorker,
		providePendingReferenceWorker,
		provideSQSConsumer,
	),
	fx.Invoke(registerLifecycle),
)

func provideLogger(config Config) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToUpper(config.LogLevel) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(logger)
	return logger
}

func providePostgresPool(config Config) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, config.DatabaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

type sqsClients struct {
	fx.Out

	Sender   worker.SQSClient
	Receiver worker.SQSReceiver
	Health   handler.SQSHealthClient
}

func provideSQSClients(config Config) (sqsClients, error) {
	awsConfig, err := awsconfig.LoadDefaultConfig(
		context.Background(),
		awsconfig.WithRegion(config.AWSRegion),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		return sqsClients{}, err
	}

	client := sqs.NewFromConfig(awsConfig, func(options *sqs.Options) {
		options.BaseEndpoint = aws.String(config.AWSEndpointURL)
	})

	return sqsClients{
		Sender:   client,
		Receiver: client,
		Health:   client,
	}, nil
}

func provideAuthMiddleware(config Config) (*middleware.AuthMiddleware, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return middleware.NewAuthMiddleware(
		ctx,
		config.KeycloakIssuerURL,
		config.KeycloakAudience,
	)
}

func provideWalletRepository() repository.WalletRepository {
	return repository.NewPostgresWalletRepository()
}

func provideTransactionRepository() repository.TransactionRepository {
	return repository.NewPostgresTransactionRepository()
}

func provideLedgerRepository() repository.LedgerRepository {
	return repository.NewPostgresLedgerRepository()
}

func provideInboxOutboxRepository() repository.InboxOutboxRepository {
	return repository.NewPostgresInboxOutboxRepository()
}

type wagerServices struct {
	fx.Out

	HTTPProcessor    handler.WagerProcessor
	Reader           handler.TransactionReader
	SQSProcessor     worker.WagerProcessor
	PendingProcessor worker.PendingReferenceProcessor
}

func provideWagerServices(
	db *pgxpool.Pool,
	walletRepository repository.WalletRepository,
	transactionRepository repository.TransactionRepository,
	ledgerRepository repository.LedgerRepository,
	inboxOutboxRepository repository.InboxOutboxRepository,
	metrics *observability.Metrics,
) wagerServices {
	wagerUseCase := usecase.NewWagerUseCase(
		db,
		walletRepository,
		transactionRepository,
		ledgerRepository,
		inboxOutboxRepository,
		metrics,
	)

	return wagerServices{
		HTTPProcessor:    wagerUseCase,
		Reader:           wagerUseCase,
		SQSProcessor:     wagerUseCase,
		PendingProcessor: wagerUseCase,
	}
}

func provideWalletApplication(
	db *pgxpool.Pool,
	walletRepository repository.WalletRepository,
	transactionRepository repository.TransactionRepository,
	ledgerRepository repository.LedgerRepository,
	inboxOutboxRepository repository.InboxOutboxRepository,
) handler.WalletApplication {
	return usecase.NewWalletUseCase(
		db,
		walletRepository,
		transactionRepository,
		ledgerRepository,
		inboxOutboxRepository,
	)
}

func provideHealthHandler(
	db *pgxpool.Pool,
	sqsClient handler.SQSHealthClient,
	config Config,
) *handler.HealthHandler {
	return handler.NewHealthHandler(
		db,
		sqsClient,
		config.SQSCommandQueueURL,
		config.SQSEventQueueURL,
	)
}

func provideHTTPServer(
	config Config,
	auth *middleware.AuthMiddleware,
	walletHandler *handler.WalletHandler,
	wagerHandler *handler.WagerHandler,
	healthHandler *handler.HealthHandler,
	metrics *observability.Metrics,
	logger *slog.Logger,
) *http.Server {
	protectedMux := http.NewServeMux()
	walletHandler.RegisterRoutes(protectedMux)
	wagerHandler.RegisterRoutes(protectedMux)

	rootMux := http.NewServeMux()
	healthHandler.RegisterRoutes(rootMux)
	rootMux.Handle("GET /metrics", metrics.Handler())
	rootMux.Handle("/wallets", auth.Authenticate(protectedMux))
	rootMux.Handle("/wallets/", auth.Authenticate(protectedMux))
	rootMux.Handle("/wagering/", auth.Authenticate(protectedMux))

	return &http.Server{
		Addr:              config.HTTPAddress,
		Handler:           metrics.HTTPMiddleware(logger, rootMux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

func provideOutboxWorker(
	db *pgxpool.Pool,
	sqsClient worker.SQSClient,
	config Config,
	metrics *observability.Metrics,
) *worker.OutboxWorker {
	return worker.NewOutboxWorker(
		db,
		sqsClient,
		config.SQSEventQueueURL,
		metrics,
	)
}

func providePendingReferenceWorker(
	processor worker.PendingReferenceProcessor,
	metrics *observability.Metrics,
) *worker.PendingReferenceWorker {
	return worker.NewPendingReferenceWorker(processor, metrics)
}

func provideSQSConsumer(
	sqsClient worker.SQSReceiver,
	processor worker.WagerProcessor,
	config Config,
	metrics *observability.Metrics,
) *worker.SQSConsumer {
	return worker.NewSQSConsumer(
		sqsClient,
		processor,
		config.SQSCommandQueueURL,
		metrics,
	)
}
