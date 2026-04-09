package exchange

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func TestGenerator(t *testing.T) {
	newGen := NewGenerator("8081")
	go func() {
		err := newGen.Start()
		if err != nil {
			t.Fatalf("failed to start generator: %v", err)
		}
	}()
	time.Sleep(200 * time.Millisecond)
	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		t.Fatalf("failed to connect to generator: %v", err)
	}
	defer conn.Close()

	buf := make([]byte, 1024)
	for i := 0; i < 10; i++ {
		n, err := conn.Read(buf)
		if err != nil {
			t.Fatalf("failed to read from generator: %v", err)
		}

		if n == 0 {
			t.Fatal("expected data from generator, got none")
		}
		fmt.Println("received data:", string(buf[:n]))
		t.Logf("received data: %s", string(buf[:n]))
	}
	newGen.quitCh <- struct{}{}
}
