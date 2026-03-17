package exchangehandler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"

	"github.com/sulte4/marketflow/internal/core/domain"
)

type Source struct {
	Name string
	Port string
	Conn net.Conn
}

func NewSource(port string) *Source {
	return &Source{
		Port: port,
	}
}

func (s *Source) Dial() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%s", s.Port))
	if err != nil {
		slog.Error("connection failed to source", slog.String("addr", s.Port))
		return err
	}

	s.Conn = conn
	s.Name = getExchangeName(s.Port)
	return nil
}

func (s *Source) Stream(ctx context.Context, out chan<- domain.Ticker) error {
	scanner := bufio.NewScanner(s.Conn)

	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var exchange domain.Ticker
		exchange.Source = s.Name

		if err := json.Unmarshal(line, &exchange); err != nil {
			slog.Error("failed to unmarshal", slog.Any("error", err))
			continue
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- exchange:
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan failed: %w", err)
	}

	return nil
}

func (s *Source) SourceName() string {
	return s.Name
}

func (s *Source) Close() error {
	return s.Conn.Close()
}

func getExchangeName(exchAddr string) string {
	return fmt.Sprintf("exchange%c", exchAddr[len(exchAddr)-1])
}
