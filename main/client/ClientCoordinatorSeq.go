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
	ClientIndex   int
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
		fmt.Println("[2] Test sequenziale avanzato")

		// Legge l'input dell'utente
		fmt.Print("Enter your choice: ")
		input, err := reader.ReadString('\n')
		if err != nil {
			if os.Getenv("DOCKER") == "1" {
				input = os.Getenv("DOCKER_OP")
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
		case "2":
			fmt.Println("Running 'Test sequenziale avanzato'...")
			advancedTestSeq()
			return
		default:
			fmt.Println("Invalid option, please try again.")
		}
	}
}

func addEndOps(replicas int) []Operation {
	ops := make([]Operation, replicas)
	for i := 0; i < replicas; i++ {
		ops[i] = Operation{ClientIndex: i, OperationType: utils.Put, Key: utils.EndKey, Value: utils.EndValue}
	}

	return ops
}

func executeOperations(index int, operations []Operation) {
	var serverName string
	var serverPort string
	var chosenServer int

	if os.Getenv("RANDOM_REPLICA") == "1" {
		r := utils.GetRandomReplica()
		serverName = utils.GetServerName(r)
		serverPort = utils.GetServerPort(r)
		chosenServer = r
	} else {
		chosenServer = index
		serverName = utils.GetServerName(chosenServer)
		serverPort = utils.GetServerPort(chosenServer)
	}

	addr := serverName + serverPort
	fmt.Printf("[CLIENT %d] Connecting to server %s\n", index, addr)

	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("[CLIENT %d] Failed to connect to server %d: %v\n", index, chosenServer, err)
		os.Exit(1)
	}
	defer func(conn *rpc.Client) {
		err := conn.Close()
		if err != nil {
			fmt.Printf("[CLIENT %d] Failed to close connection to server %d: %v\n", index, chosenServer, err)
		}
	}(conn)

	// Crea un WaitGroup per sincronizzare tutte le goroutine
	var wg sync.WaitGroup
	wg.Add(len(operations) / utils.NumberOfReplicas) // Incrementa il contatore per ogni chiamata

	// Contatore per la numerazione delle richieste
	var requestNumber int

	for _, op := range operations {
		if op.ClientIndex != index {
			continue
		}

		requestNumber++
		args := utils.NewArg(op.Key, op.Value, requestNumber, index)
		resp := utils.NewResponse()

		// Esegui la chiamata in una goroutine
		go func(opType string, args *utils.Args, resp *utils.Response) {
			defer wg.Done() // Decrementa il contatore al termine della chiamata
			var err error

			switch opType {
			case utils.Put:
				if op.Key == utils.EndKey && op.Value == utils.EndValue {
					time.Sleep(5 * time.Second) //Sono op. speciali che servono solo a sbloccare l'ultima exec. Devono necessariamente essere le ultime
				}
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
				fmt.Printf("[CLIENT %d] Answer from server: GET of Key '%s' Value = %s\n", index, resp.Key, resp.Value)
			}

		}(op.OperationType, args, resp)

	}

	wg.Wait() // Aspetta che tutte le goroutine abbiano finito
	fmt.Printf("[CLIENT %d] Done\n", index)

}
