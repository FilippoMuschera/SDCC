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

var consistType = strings.ToLower(os.Getenv("CONSIST_TYPE"))

func main() {
	reader := bufio.NewReader(os.Stdin) // Crea un lettore per leggere l'input dell'utente
	for {
		// Mostra il prompt all'utente
		fmt.Println("Selezionare il test che si vuole eseguire:")
		if consistType == "sequential" {
			fmt.Println("[1] Test sequenziale base")
			fmt.Println("[2] Test sequenziale avanzato")
		} else if consistType == "causal" {
			fmt.Println("[3] Test causale base")
			fmt.Println("[4] Test causale avanzato")

		} else {
			fmt.Println("You have an error in you environment configuration: value of 'CONSIST_TYPE' env variable not set or invalid")
			os.Exit(1)
		}
		fmt.Println("[5] Esci")
		printMenuExplanation()

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
		case "3":
			fmt.Println("Running 'Test causale base'...")
			basicCasualTest()
			return
		case "4":
			fmt.Println("Running 'Test causale avanzato'...")
			advancedCasualTest()
			return
		case "5":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid option, please try again.")
		}
	}
}

func printMenuExplanation() {
	// Codice ANSI per il giallo scuro (Yellow = 33)
	yellow := "\033[33m"
	reset := "\033[0m"

	// Ottieni il tipo di consistenza dall'ambiente
	consistType := os.Getenv("CONSIST_TYPE")

	if consistType == "Sequential" {
		fmt.Printf(yellow + "\n╔════════════════════════════════════════════════════════════════════════════════════════════╗\n")
		fmt.Printf("║ N.B.: Avendo selezionato la consistenza 'Sequential', sono visibili solo i test [1] e [2]. ║\n")
		fmt.Printf("║ Per vedere gli altri, assicurati di cambiare il tipo di consistenza scelta!                ║\n")
		fmt.Printf("╚════════════════════════════════════════════════════════════════════════════════════════════╝\n" + reset)

	} else if consistType == "Causal" {
		fmt.Printf(yellow + "\n╔════════════════════════════════════════════════════════════════════════════════════════════╗\n")
		fmt.Printf("║ N.B.: Avendo selezionato la consistenza 'Causal', sono visibili solo i test [3] e [4].     ║\n")
		fmt.Printf("║ Per vedere gli altri, assicurati di cambiare il tipo di consistenza scelta!                ║\n")
		fmt.Printf("╚════════════════════════════════════════════════════════════════════════════════════════════╝\n" + reset)
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
				err = conn.Call(consistType+".Put", args, resp)
			case utils.Get:
				err = conn.Call(consistType+".Get", args, resp)
			case utils.Delete:
				err = conn.Call(consistType+".Delete", args, resp)
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
