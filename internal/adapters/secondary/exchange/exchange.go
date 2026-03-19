package exchange

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
	slog.Info("attempting to connect to exchange source",
		slog.String("port", s.Port))
	
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%s", s.Port))
	if err != nil {
		slog.Error("connection failed to source",
			slog.String("port", s.Port),
			slog.String("error", err.Error()))
		return err
	}

	s.Conn = conn
	s.Name = getExchangeName(s.Port)
	
	slog.Info("successfully connected to exchange source",
		slog.String("exchange", s.Name),
		slog.String("port", s.Port))
	
	return nil
}

func (s *Source) Stream(ctx context.Context, out chan<- domain.Ticker) error {
	slog.Info("starting stream from exchange",
		slog.String("exchange", s.Name),
		slog.String("port", s.Port))
	
	scanner := bufio.NewScanner(s.Conn)

	for scanner.Scan() {
		if ctx.Err() != nil {
			slog.Warn("stream context cancelled",
				slog.String("exchange", s.Name),
				slog.String("error", ctx.Err().Error()))
			return ctx.Err()
		}
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var exchange domain.Ticker
		exchange.Source = s.Name

		if err := json.Unmarshal(line, &exchange); err != nil {
			slog.Error("failed to unmarshal ticker data",
				slog.String("exchange", s.Name),
				slog.String("error", err.Error()),
				slog.String("raw_data", string(line)))
			continue
		}

		select {
		case <-ctx.Done():
			slog.Warn("stream context cancelled during send",
				slog.String("exchange", s.Name))
			return ctx.Err()
		case out <- exchange:
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Error("scanner error in stream",
			slog.String("exchange", s.Name),
			slog.String("error", err.Error()))
		return fmt.Errorf("scan failed: %w", err)
	}

	slog.Info("stream ended",
		slog.String("exchange", s.Name))
	return nil
}

func (s *Source) SourceName() string {
	return s.Name
}

func (s *Source) Close() error {
	slog.Info("closing connection to exchange source",
		slog.String("exchange", s.Name),
		slog.String("port", s.Port))
	
	err := s.Conn.Close()
	if err != nil {
		slog.Error("failed to close connection",
			slog.String("exchange", s.Name),
			slog.String("error", err.Error()))
		return err
	}
	
	slog.Info("connection closed successfully",
		slog.String("exchange", s.Name))
	return nil
}

func getExchangeName(exchAddr string) string {
	return fmt.Sprintf("exchange%c", exchAddr[len(exchAddr)-1])
}
