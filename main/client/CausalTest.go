package main

import (
	"SDCC/main/utils"
	"fmt"
	"os"
	"sync"
	"time"
)

func basicCasualTest() {
	fmt.Println("In questo test causale, le seguenti operazioni vengono inviate in parallelo ai server:\n")

	fmt.Println(" +-----------+-----------+-----------+")
	fmt.Println(" | Operazione|     1     |     2     |")
	fmt.Println(" +-----------+-----------+-----------+")
	fmt.Println(" | Processo 0| put x:1   | put y:2   |")
	fmt.Println(" +-----------+-----------+-----------+")
	fmt.Println(" | Processo 1| get x     | put x:3   |")
	fmt.Println(" +-----------+-----------+-----------+")
	fmt.Println(" | Processo 2| get y     | put y:4   |")
	fmt.Println(" +-----------+-----------+-----------+")

	operations := []Operation{
		// Operazioni per Processo 0
		{ClientIndex: 0, OperationType: utils.Put, Key: "x", Value: "1"},
		{ClientIndex: 0, OperationType: utils.Put, Key: "y", Value: "2"},

		// Operazioni per Processo 1
		{ClientIndex: 1, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 1, OperationType: utils.Put, Key: "x", Value: "3"},

		// Operazioni per Processo 2
		{ClientIndex: 2, OperationType: utils.Get, Key: "y"},
		{ClientIndex: 2, OperationType: utils.Put, Key: "y", Value: "4"},
	}
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

func advancedCasualTest() {
	fmt.Println("In questo test causale, le seguenti operazioni vengono inviate in parallelo ai server:\n")

	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Operazione|     1     |     2     |     3     |     4     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Processo 0| Get x     | Put y:a   | Get x     | Put x:e   |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Processo 1| Put x:b   | Put z:c   | Get y     | Put x:d   |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	fmt.Println(" | Processo 2| Get y     | Put y:f   | Get x     | Del z     |")
	fmt.Println(" +-----------+-----------+-----------+-----------+-----------+")
	operations := []Operation{
		// Operazioni per Processo 1
		{ClientIndex: 0, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 0, OperationType: utils.Put, Key: "y", Value: "a"},
		{ClientIndex: 0, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 0, OperationType: utils.Put, Key: "x", Value: "e"},

		// Operazioni per Processo 2
		{ClientIndex: 1, OperationType: utils.Put, Key: "x", Value: "b"},
		{ClientIndex: 1, OperationType: utils.Put, Key: "z", Value: "c"},
		{ClientIndex: 1, OperationType: utils.Get, Key: "y"},
		{ClientIndex: 1, OperationType: utils.Put, Key: "x", Value: "d"},

		// Operazioni per Processo 3
		{ClientIndex: 2, OperationType: utils.Get, Key: "y"},
		{ClientIndex: 2, OperationType: utils.Put, Key: "y", Value: "f"},
		{ClientIndex: 2, OperationType: utils.Get, Key: "x"},
		{ClientIndex: 2, OperationType: utils.Delete, Key: "z"},
	}

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
