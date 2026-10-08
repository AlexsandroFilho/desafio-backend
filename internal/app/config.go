package app

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	HTTPAddress        string
	DatabaseURL        string
	AWSRegion          string
	AWSEndpointURL     string
	SQSCommandQueueURL string
	SQSEventQueueURL   string
	KeycloakIssuerURL  string
	KeycloakAudience   string
	LogLevel           string
}

func LoadConfig() (Config, error) {
	if err := godotenv.Load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Config{}, err
	}

	config := Config{
		HTTPAddress:        environmentOrDefault("HTTP_ADDR", ":8081"),
		DatabaseURL:        environmentOrDefault("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/wagerdb?sslmode=disable"),
		AWSRegion:          environmentOrDefault("AWS_REGION", "us-east-1"),
		AWSEndpointURL:     environmentOrDefault("AWS_ENDPOINT_URL", "http://localhost:4566"),
		SQSCommandQueueURL: environmentOrDefault("SQS_COMMAND_QUEUE_URL", "http://localhost:4566/000000000000/wager-transactions.fifo"),
		SQSEventQueueURL:   environmentOrDefault("SQS_EVENT_QUEUE_URL", "http://localhost:4566/000000000000/wager-events.fifo"),
		KeycloakIssuerURL:  environmentOrDefault("KEYCLOAK_ISSUER_URL", "http://localhost:8080/realms/wager"),
		KeycloakAudience:   environmentOrDefault("KEYCLOAK_AUDIENCE", "wager-api"),
		LogLevel:           environmentOrDefault("LOG_LEVEL", "INFO"),
	}

	if config.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if config.SQSCommandQueueURL == "" || config.SQSEventQueueURL == "" {
		return Config{}, errors.New("SQS command and event queue URLs are required")
	}
	if config.KeycloakIssuerURL == "" || config.KeycloakAudience == "" {
		return Config{}, errors.New("Keycloak issuer and audience are required")
	}

	return config, nil
}

func environmentOrDefault(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}
