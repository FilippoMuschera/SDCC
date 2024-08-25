package main

import (
	"SDCC/main/utils"
	"bufio"
	"fmt"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type Operation struct {
	ServerIndex   int
	OperationType string
	Key           string
	Value         string
}

// Barrier Step 1: Define a Barrier struct.
type Barrier struct {
	total int
	count int
	mutex sync.Mutex
	cond  *sync.Cond
}

// NewBarrier Step 2: Implement the Barrier struct
func NewBarrier(count int) *Barrier {
	b := &Barrier{
		total: count,
		count: 0,
	}
	b.cond = sync.NewCond(&b.mutex)
	return b
}

// Wait Step 3: Define a Wait method that goroutines can call when they reach the barrier.
func (b *Barrier) Wait() {
	b.mutex.Lock()
	b.count++
	if b.count >= b.total {
		b.count = 0
		time.Sleep(400 * time.Millisecond)
		b.cond.Broadcast()
	} else {
		b.cond.Wait()
	}
	b.mutex.Unlock()
}
func main() {
	reader := bufio.NewReader(os.Stdin) // Crea un lettore per leggere l'input dell'utente
	for {
		// Mostra il prompt all'utente
		fmt.Println("Select the type of test you want to run:")
		fmt.Println("[1] Test sequenziale base")

		// Legge l'input dell'utente
		fmt.Print("Enter your choice: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			if os.Getenv("DOCKER") == "1" {
				input = os.Args[1]
			} else {
				fmt.Println("Error reading input, please try again.")
				continue
			}
		}

		// Rimuovi spazi bianchi e newline
		input = strings.TrimSpace(input)

		// Verifica l'opzione scelta
		switch input {
		case "1":
			fmt.Println("Running 'Test sequenziale base'...")
			basicTestSeq() // Chiama la funzione per eseguire il test sequenziale base
			return
		default:
			fmt.Println("Invalid option, please try again.")
		}
	}
}

func addEndOps(replicas int) []Operation {
	ops := make([]Operation, replicas)
	for i := 0; i < replicas; i++ {
		ops[i] = Operation{ServerIndex: i, OperationType: utils.Put, Key: utils.EndKey, Value: utils.EndValue}
	}

	return ops
}

func executeOperations(index int, operations []Operation, barrier *Barrier) {
	serverName := utils.GetServerName(index)
	serverPort := utils.GetServerPort(index)

	addr := serverName + serverPort
	fmt.Printf("[CLIENT %d] Connecting to server %s\n", index, addr)

	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("[CLIENT %d] Failed to connect to server %d: %v\n", index, index, err)
		os.Exit(1)
	}
	defer func(conn *rpc.Client) {
		err := conn.Close()
		if err != nil {
			fmt.Printf("[CLIENT %d] Failed to close connection to server %d: %v\n", index, index, err)
		}
	}(conn)

	// Crea un WaitGroup per sincronizzare tutte le goroutine
	var wg sync.WaitGroup

	// Contatore per la numerazione delle richieste
	var requestNumber int

	for _, op := range operations {
		if op.ServerIndex != index {
			continue
		}

		requestNumber++
		args := utils.NewArg(op.Key, op.Value, requestNumber, index)
		resp := utils.NewResponse()

		//fmt.Printf("[CLIENT %d] Requesting op %s with req number = %d\n", index, op.OperationType, requestNumber)

		// Esegui la chiamata in una goroutine
		wg.Add(1) // Incrementa il contatore per ogni chiamata
		go func(opType string, args *utils.Args, resp *utils.Response) {
			defer wg.Done() // Decrementa il contatore al termine della chiamata
			var err error

			switch opType {
			case utils.Put:
				err = conn.Call("sequential.Put", args, resp)
			case utils.Get:
				err = conn.Call("sequential.Get", args, resp)
			case utils.Delete:
				err = conn.Call("sequential.Delete", args, resp)
			}

			if err != nil {
				fmt.Printf("[CLIENT %d] Error in call to %s: %v\n", index, opType, err)
				return
			}

			if resp.IsPrintable {
				fmt.Printf("[CLIENT %d] Answer from server: GET Value = %s\n", index, resp.Value)
			}

		}(op.OperationType, args, resp)

		barrier.Wait()

	}

	wg.Wait() // Aspetta che tutte le goroutine abbiano finito
	fmt.Printf("[CLIENT %d] Done\n", index)

}
