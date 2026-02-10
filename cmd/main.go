package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	exchangehandler "github.com/sulte4/marketflow/internal/adapters/exchangeHandler"
	"github.com/sulte4/marketflow/internal/domain/model"
	"github.com/sulte4/marketflow/pkg/config"
)

type batchedData struct{
	Min float64
	Max float64
	Sum float64
	Count int
}

func main() {
	ctx := context.Background()
	cfg, err:= config.LoadConfig()
	if err != nil {
		slog.Error(err.Error())
		return 
	}
	
	outs := make([]chan []model.Exchange, 3)
	for i, exch := range cfg.Exchanges{
		out := make(chan []model.Exchange)
		outs[i] = out
		go func (){
			err := exchangehandler.ConnExchange(ctx, out, exch)
			if err != nil {
				slog.Error(err.Error())
			}
		}()
	}
	wg := &sync.WaitGroup{}
	for _, out := range outs{
		wg.Add(1)
		go func(){
			for {

				fmt.Println(<-out)
			}
		}()
	}
	wg.Wait()
}

func fanOut(in <-chan model.Exchange, n int) []chan model.Exchange{
	outs := make([]chan model.Exchange, n)
	for i := range n{
		out := make(chan model.Exchange)
		outs[i] = out

		go func(){
			defer close(out)
			for val := range in{
				out <- val
			}
		}()
	}
	return outs
}

func fanIn(ctx context.Context, chans []chan model.Exchange) chan model.Exchange{
	out := make(chan model.Exchange)

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
						case out <- v:
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
		close(out)
	}()

	return out
}
// func worker(in <- chan []model.Exchange, out chan <- []model.Exchange){

// }