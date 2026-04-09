package exchange

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/sulte4/marketflow/internal/core/domain"
)

var (
	tickersCount = 5
	tickers      = []string{"BTCUSDT", "DOGEUSDT", "TONUSDT", "ETHUSDT", "SOLUSDT"}
	prices       = []float64{70900.0, 0.93, 1.243, 2171.57, 82.49} // taken from real data on 2024-04-09 at 15:00 PM UTC
)

// implement option pattern for generator configuration
// delay between ticks in milliseconds
type Generator struct {
	port     string
	listener net.Listener
	quitCh   chan struct{}
	delay    int
}

// type Option func(*Generator)

func NewGenerator(port string) *Generator {
	return &Generator{
		port:   port,
		quitCh: make(chan struct{}),
		delay:  10,
	}
}

func (g *Generator) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", g.port))
	if err != nil {
		slog.Error("failed to start generator",
			slog.String("port", g.port),
			slog.String("error", err.Error()))
		return err
	}
	defer listener.Close()

	g.listener = listener

	slog.Info("generator started successfully",
		slog.String("port", g.port))
	go g.acceptLoop()

	<-g.quitCh
	return nil
}

func (g *Generator) acceptLoop() {
	for {
		conn, err := g.listener.Accept()
		if err != nil {
			slog.Error("failed to accept connection",
				slog.String("error", err.Error()))
			continue
		}

		go g.writeLoop(conn)
	}
}

func (g *Generator) writeLoop(conn net.Conn) {
	defer conn.Close()
	for {
		for i := 0; i < tickersCount; i++ {
			newPrice := g.nextPrice(prices[i])
			tick := domain.Ticker{
				Symbol:    tickers[i],
				Price:     float32(newPrice),
				Timestamp: time.Now().Unix(),
			}
			prices[i] = newPrice

			buf, err := json.Marshal(tick)
			if err != nil {
				slog.Error("failed to marshal ticker data",
					slog.String("error", err.Error()))
				continue
			}
			buf = fmt.Appendf(buf[:0], "%s\n", string(buf))

			conn.Write(buf)
			time.Sleep(time.Duration(g.delay) * time.Millisecond)
		}
	}
}

func (g *Generator) nextPrice(price float64) float64 {
	return price
}
