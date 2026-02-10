// pkg/config/config.go
package config

import (
	"errors"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Exchanges []string
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

    return &Config{
        Exchanges: exchanges,
    }, nil
}