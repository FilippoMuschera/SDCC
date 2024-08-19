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
			fmt.Println("Error reading input, please try again.")
			continue
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

		/*{ServerIndex: 0, OperationType: utils.Delete, Key: "x"},
		{ServerIndex: 1, OperationType: utils.Delete, Key: "x"},
		{ServerIndex: 2, OperationType: utils.Delete, Key: "x"},

		{ServerIndex: 0, OperationType: utils.Get, Key: "x"},
		{ServerIndex: 1, OperationType: utils.Get, Key: "x"},
		{ServerIndex: 2, OperationType: utils.Get, Key: "x"},*/
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

	// Channel to receive RPC responses
	doneChan := make(chan *rpc.Call, len(operations))

	// Contatore per la numerazione delle richieste
	var requestNumber int

	// Invio delle operazioni filtrate per questo client
	for _, op := range operations {
		if op.ServerIndex != index {
			continue
		}
		time.Sleep(500 * time.Millisecond) //TODO fare vera synch, non attese
		requestNumber++
		args := utils.NewArg(op.Key, op.Value, requestNumber, index)
		resp := utils.NewResponse()

		fmt.Printf("\033[36m[CLIENT %d] Requesting to server %s. Operation %s with key %s [RN: %d]\033[0m\n", index, addr, op.OperationType, args.Key, requestNumber)

		switch op.OperationType {
		case utils.Put:
			conn.Go("sequential.Put", args, resp, doneChan)
		case utils.Get:
			conn.Go("sequential.Get", args, resp, doneChan)
		case utils.Delete:
			conn.Go("sequential.Delete", args, resp, doneChan)
		}
	}

	// Attesa delle risposte
	for i := 0; i < requestNumber-1; i++ { // -1: IGNORO L'ENDMESSAGE PERCHÈ È UNA SORTA DI "DISCONNECT", NON UN VERO MESSAGGIO CHE RICHIEDE UNA RISPOSTA
		call := <-doneChan
		if call.Error != nil {
			fmt.Printf("[CLIENT %d] Error in call to %s: %v\n", index, operations[i].OperationType, call.Error)
			continue
		}
		// Asserzione di tipo per verificare che `call.Reply` sia effettivamente un `*utils.Response`
		if response, ok := call.Reply.(*utils.Response); ok {
			// Se l'asserzione ha successo, controlla se `IsPrintable` è true e stampa il valore
			if response.IsPrintable {
				fmt.Printf("[CLIENT %d] Answer from server: GET Value = %s\n", index, response.Value)
			}
		} else {
			// Se l'asserzione fallisce, stampa un messaggio di errore
			fmt.Printf("[CLIENT %d] Error: unable to assert type to *utils.Response\n", index)
		}

		fmt.Printf("[CLIENT %d] Done\n", index)
	}

}
