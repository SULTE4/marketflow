package main

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	exchangehandler "github.com/sulte4/marketflow/internal/adapters/exchangeHandler"
	"github.com/sulte4/marketflow/internal/domain/model"
	"github.com/sulte4/marketflow/pkg/config"
)

type batchedData struct{
	Min float32
	Max float32
	Sum float64
	Count int
	Source string
}
func (v *batchedData) Calculation(exchange model.Exchange){
	v.Count++
	v.Min = min(v.Min, exchange.Price)
	v.Max = max(v.Max, exchange.Price)
	v.Sum += float64(exchange.Price)
}
func (v *batchedData) Reset(){
	v.Min = math.MaxFloat32 
	v.Max = -math.MaxFloat32
	v.Count = 0
	v.Sum = 0
}

func main() {
	ctx := context.Background()
	cfg, err:= config.LoadConfig()
	if err != nil {
		slog.Error(err.Error())
		return 
	}
	
	resultCh := make(chan model.Exchange)
	sources := make([]chan []model.Exchange, 3)
	for i, exch := range cfg.Exchanges{
		source := make(chan []model.Exchange)
		sources[i] = source
		go func (){
			err := exchangehandler.ConnExchange(ctx, source, exch)
			if err != nil {
				slog.Error(err.Error())
			}
		}()
	}	
	
	outs := make([]chan model.Exchange, 0, 15)
	for _, source := range sources{
		chs := fanOut(ctx, source, 5)
		outs = append(outs, chs...)	
	}
	fanIn(ctx, outs, resultCh)

	
}

func fanOut(ctx context.Context, in <-chan []model.Exchange, n int) []chan model.Exchange{
	outs := make([]chan model.Exchange, n)
	for i := range n{
		out := make(chan model.Exchange)
		outs[i] = out
		go worker(ctx, in, out, i)
	}
	return outs
}

func fanIn(ctx context.Context, chans []chan model.Exchange, res chan model.Exchange) {

	go func(){
		wg := &sync.WaitGroup{}
		for _, ch := range chans{
			wg.Add(1)
			go func(ch chan model.Exchange) {
				defer wg.Done()
				for{
					select{
					case v, ok := <- ch:
						if !ok{
							return
						}
						select{
						case res <- v:
						case <- ctx.Done():
							return
						}
					case <- ctx.Done():
						return
					}
				}

			}(ch)
		}
		wg.Wait()
		close(res)
	}()

}

func worker(ctx context.Context, in <-chan []model.Exchange, out chan<- model.Exchange, pairIndex int){
	batched := batchedData{}
	ticker := time.NewTicker(time.Minute * 1)
	defer close(out)
	for{
		select {
		case pair, ok := <- in:
			if !ok{
				slog.Info("worker stopped due channel close")
				return
			}
			batched.Calculation(pair[pairIndex])
			//adding to redis
		case <- ticker.C:
			//adding to postgres
			fmt.Println("adding to postgres",batched)
			batched.Reset()
		case <- ctx.Done():	
			ticker.Stop()
			return	
		}
	}
}
