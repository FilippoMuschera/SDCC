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

func basicTestSeq() {
	fmt.Println("In questo test sequenziale, le seguenti operazioni vengono inviate in parallelo ai server:\n")

	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Operazione|     1     |     2     |     3     |     4     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Server 1  | put x:1   | get x     | del x     | get x     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Server 2  | put x:2   | get x     | del x     | get x     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Server 3  | put x:3   | get x     | del x     | get x     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")

	operations := []Operation{
		{0, utils.Put, "x", "1"},
		{1, utils.Put, "x", "2"},
		{2, utils.Put, "x", "3"},

		{ServerIndex: 0, OperationType: utils.Get, Key: "x"},
		{ServerIndex: 1, OperationType: utils.Get, Key: "x"},
		{ServerIndex: 2, OperationType: utils.Get, Key: "x"},

		{ServerIndex: 0, OperationType: utils.Delete, Key: "x"},
		{ServerIndex: 1, OperationType: utils.Delete, Key: "x"},
		{ServerIndex: 2, OperationType: utils.Delete, Key: "x"},

		{ServerIndex: 0, OperationType: utils.Get, Key: "x"},
		{ServerIndex: 1, OperationType: utils.Get, Key: "x"},
		{ServerIndex: 2, OperationType: utils.Get, Key: "x"},
	}
	operations = append(operations, addEndOps(utils.NumberOfReplicas)...)
	// Creazione di un WaitGroup
	var wg sync.WaitGroup

	// Aggiungi il numero di goroutine che aspettiamo
	wg.Add(utils.NumberOfReplicas)

	// Lancio delle goroutine
	for i := 0; i < utils.NumberOfReplicas; i++ {
		go func(index int) {
			defer wg.Done()                               // Indica che questa goroutine è completata quando esce dalla funzione
			executeSequentialOperation(index, operations) // Esegui l'operazione
		}(i)
	}

	// Attendi che tutte le goroutine completino l'esecuzione
	wg.Wait()

	fmt.Println("All operations have completed.")

	if os.Getenv("DOCKER") == "1" {
		time.Sleep(1 * time.Hour) //Rimane attivo per permettere di accedere al log

	}
}

func addEndOps(replicas int) []Operation {
	ops := make([]Operation, replicas)
	for i := 0; i < replicas; i++ {
		ops[i] = Operation{ServerIndex: i, OperationType: utils.Put, Key: utils.EndKey, Value: utils.EndValue}
	}

	return ops
}

func executeSequentialOperation(index int, operations []Operation) {
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
		time.Sleep(500 * time.Millisecond)
	}

	wg.Wait() // Aspetta che tutte le goroutine abbiano finito
	fmt.Printf("[CLIENT %d] Done\n", index)

}
