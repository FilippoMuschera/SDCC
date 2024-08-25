package main

import (
	"SDCC/main/utils"
	"fmt"
	"os"
	"sync"
	"time"
)

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
	barrier := NewBarrier(utils.NumberOfReplicas)

	// Lancio delle goroutine
	for i := 0; i < utils.NumberOfReplicas; i++ {
		go func(index int) {
			defer wg.Done()                               // Indica che questa goroutine è completata quando esce dalla funzione
			executeOperations(index, operations, barrier) // Esegui l'operazione
		}(i)
	}

	// Attendi che tutte le goroutine completino l'esecuzione
	wg.Wait()

	fmt.Println("All operations have completed.")

	if os.Getenv("DOCKER") == "1" {
		time.Sleep(1 * time.Hour) //Rimane attivo per permettere di accedere al log

	}
}
