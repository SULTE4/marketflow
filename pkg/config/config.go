// pkg/config/config.go
package config

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Exchanges  []string
	RedisHost  string
	RedisPort  string
	DBHost     string
	DBPort     string
	DBUser     string
	DBPassword string
	DBName     string
	Addr       string
}

func LoadConfig() (*Config, error) {
	envPath := ".env"
	if _, err := os.Stat(envPath); os.IsNotExist(err) {
		envPath = "cmd/.env"
	}

	_ = godotenv.Load(envPath)

	exchanges := make([]string, 3)
	exchanges[0] = os.Getenv("EXCHANGE1")
	exchanges[1] = os.Getenv("EXCHANGE2")
	exchanges[2] = os.Getenv("EXCHANGE3")

	if exchanges[0] == "" || exchanges[1] == "" || exchanges[2] == "" {
		return nil, errors.New("missing required environment variables: EXCHANGE1, EXCHANGE2, EXCHANGE3")
	}

	dbHost := os.Getenv("DB_HOST")
	dbPort := os.Getenv("DB_PORT")
	dbUser := os.Getenv("DB_USER")
	dbPassword := os.Getenv("DB_PASSWORD")
	dbName := os.Getenv("DB_NAME")

	if dbHost == "" || dbPort == "" || dbUser == "" || dbPassword == "" || dbName == "" {
		return nil, errors.New("missing required environment variables: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME")
	}

	redisHost := os.Getenv("REDIS_HOST")
	redisPort := os.Getenv("REDIS_PORT")

	if redisHost == "" || redisPort == "" {
		return nil, errors.New("missing required environment variables: REDIS_HOST, REDIS_PORT")
	}

	addr := os.Getenv("HTTP_ADDR")

	if addr == "" {
		return nil, errors.New("missing required environment variable: HTTP_ADDR")
	}

	return &Config{
		Exchanges:  exchanges,
		DBHost:     dbHost,
		DBPort:     dbPort,
		DBUser:     dbUser,
		DBPassword: dbPassword,
		DBName:     dbName,
		RedisHost:  redisHost,
		RedisPort:  redisPort,
		Addr:       addr,
	}, nil
}
