package exchangehandler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"

	"github.com/sulte4/marketflow/internal/domain/model"
)

func ConnExchange(ctx context.Context, out  chan<- []model.Exchange, addr string)  error{
	defer close(out)
	
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%s", addr) )	
	if err != nil {
		slog.Error("connection failed to source", slog.String("addr", addr))
		return  err
	}
	
	defer conn.Close()
	slog.Info("connected successfully to exchange")

	exchName := getExchangeName(addr)
	exchanges := make([]model.Exchange, 5) 
	count := 0

	scanner := bufio.NewScanner(conn)

	for scanner.Scan(){
		if ctx.Err() != nil{
			return nil
		}
		
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0{
			continue
		}
		
		var exchange model.Exchange
		exchange.Source = exchName

		err = json.Unmarshal(line, &exchange)
		if err != nil{
			slog.Error("failed to unmarshal", slog.String("addr", exchName), slog.Any("error", err))
			continue
		}
		
		exchanges[count] = exchange
		count++
		
		if count == 5{
			count = 0
			select{
			case out <- exchanges:
			case <- ctx.Done():
				return nil 
			}
		}
	}
	if err := scanner.Err(); err != nil{
		return fmt.Errorf("scan failed: %w", err)
	}
	return nil
}

func getExchangeName(exchAddr string) string {
	return fmt.Sprintf("Exchange%c", exchAddr[len(exchAddr) - 1])
} 