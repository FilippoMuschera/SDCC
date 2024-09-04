package main

import (
	"SDCC/main/utils"
	"fmt"
	"os"
	"sync"
	"time"
)

func init() {
	if os.Getenv("RANDOM_REPLICA") == "1" {
		fmt.Print("\033[36mUsing a random server for every client\n\033[0m")
	}
}

func basicTestSeq() {
	fmt.Println("In questo test sequenziale, le seguenti operazioni vengono inviate in parallelo ai server:\n")

	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Operazione|     1     |     2     |     3     |     4     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Processo 0| put x:1   | get x     | del x     | get x     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Processo 1| put x:2   | get x     | del x     | get x     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Processo 2| put x:3   | get x     | del x     | get x     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")

	operations := []Operation{
		// Operazioni per Processo 0
		{ClientIndex: 0, OperationType: utils.Put, Key: "x", Value: "1"},
		{ClientIndex: 0, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 0, OperationType: utils.Delete, Key: "x"},
		{ClientIndex: 0, OperationType: utils.Get, Key: "x"},

		// Operazioni per Processo 1
		{ClientIndex: 1, OperationType: utils.Put, Key: "x", Value: "2"},
		{ClientIndex: 1, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 1, OperationType: utils.Delete, Key: "x"},
		{ClientIndex: 1, OperationType: utils.Get, Key: "x"},

		// Operazioni per Processo 2
		{ClientIndex: 2, OperationType: utils.Put, Key: "x", Value: "3"},
		{ClientIndex: 2, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 2, OperationType: utils.Delete, Key: "x"},
		{ClientIndex: 2, OperationType: utils.Get, Key: "x"},
	}
	operations = append(operations, addEndOps(utils.NumberOfReplicas)...)
	// Creazione di un WaitGroup
	var wg sync.WaitGroup

	// Aggiungi il numero di goroutine che aspettiamo
	wg.Add(utils.NumberOfReplicas)

	// Lancio delle goroutine
	for i := 0; i < utils.NumberOfReplicas; i++ {
		go func(index int) {
			defer wg.Done()                      // Indica che questa goroutine è completata quando esce dalla funzione
			executeOperations(index, operations) // Esegui l'operazione
		}(i)
	}

	// Attendi che tutte le goroutine completino l'esecuzione
	wg.Wait()

	fmt.Println("All operations have completed.")

	if os.Getenv("DOCKER") == "1" {
		time.Sleep(1 * time.Hour) //Rimane attivo per permettere di accedere al log

	}
}

func advancedTestSeq() {
	fmt.Println("In questo test sequenziale, le seguenti operazioni vengono inviate in parallelo ai server:\n")

	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Operazione|     1     |     2     |     3     |     4     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Processo 0| put x:1   | get y     | get x     | get z     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Processo 1| put y:2   | get x     | get z     | put z:5   |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Processo 2| get x     | put x:3   | put z:4   | del x     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")

	operations := []Operation{
		// Operazioni per Processo 0
		{ClientIndex: 0, OperationType: utils.Put, Key: "x", Value: "1"},
		{ClientIndex: 0, OperationType: utils.Get, Key: "y"},
		{ClientIndex: 0, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 0, OperationType: utils.Get, Key: "z"},

		// Operazioni per Processo 1
		{ClientIndex: 1, OperationType: utils.Put, Key: "y", Value: "2"},
		{ClientIndex: 1, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 1, OperationType: utils.Get, Key: "z"},
		{ClientIndex: 1, OperationType: utils.Put, Key: "z", Value: "5"},

		// Operazioni per Processo 2
		{ClientIndex: 2, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 2, OperationType: utils.Put, Key: "x", Value: "3"},
		{ClientIndex: 2, OperationType: utils.Put, Key: "z", Value: "4"},
		{ClientIndex: 2, OperationType: utils.Delete, Key: "x"},
	}

	operations = append(operations, addEndOps(utils.NumberOfReplicas)...)
	// Creazione di un WaitGroup
	var wg sync.WaitGroup

	// Aggiungi il numero di goroutine che aspettiamo
	wg.Add(utils.NumberOfReplicas)

	// Lancio delle goroutine
	for i := 0; i < utils.NumberOfReplicas; i++ {
		go func(index int) {
			defer wg.Done()                      // Indica che questa goroutine è completata quando esce dalla funzione
			executeOperations(index, operations) // Esegui l'operazione
		}(i)
	}

	// Attendi che tutte le goroutine completino l'esecuzione
	wg.Wait()

	fmt.Println("All operations have completed.")

	if os.Getenv("DOCKER") == "1" {
		time.Sleep(1 * time.Hour) //Rimane attivo per permettere di accedere al log

	}
}
