package main

import (
	"SDCC/main/utils"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
)

// KVSSequential is a concrete implementation of the KVS interface
type KVSSequential struct {
	store      map[string]string
	mu         sync.Mutex //mutex per accedere alla Map
	clientList ClientList //lista dei client per il singolo server
}

type ClientList struct {
	list []int
	mu   sync.Mutex
}

// NewKVSSequential creates a new instance of KVSSequential
func NewKVSSequential() *KVSSequential {
	numOfClients, _ := strconv.Atoi(os.Getenv("REPLICAS"))
	return &KVSSequential{
		store: make(map[string]string),
		clientList: ClientList{
			list: make([]int, numOfClients),
		},
	}
}

// Get retrieves a value from the key-value store
func (kvs *KVSSequential) Get(args utils.Args, reply *utils.Response) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	fmt.Println("Serving GET operation")

	value, ok := kvs.store[args.Key]
	if !ok {
		return errors.New("key not found")
	}
	fmt.Println("value in GET = ", value)

	reply.Value = value
	reply.IsPrintable = true
	return nil
}

// Put stores a value in the key-value store
func (kvs *KVSSequential) Put(args utils.Args, reply *utils.Response) error {
	kvs.mu.Lock()
	fmt.Println("Serving PUT operation")
	defer kvs.mu.Unlock()

	kvs.store[args.Key] = args.Value
	reply.Value = ""
	return nil
}

// Delete removes a key from the key-value store
func (kvs *KVSSequential) Delete(args utils.Args, reply *utils.Response) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	_, ok := kvs.store[args.Key]
	if !ok {
		return errors.New("key not found")
	}

	delete(kvs.store, args.Key)
	reply.Value = ""
	return nil
}

// EstablishFirstConnection serve ad inizializzare la connessione client-server
func (kvs *KVSSequential) EstablishFirstConnection(args utils.Args, reply *utils.Response) error {
	/* Per stabilire una nuova connessione client-server, il server deve andare
	 * a creare una apposita entry per tenere traccia del numero di messaggi ricevuti
	 * dal client. Questo è necessario per rispettare l'assunzione FIFO dell'ordinamento
	 * dei messaggi.
	 */
	index, _ := strconv.Atoi(args.Value)
	kvs.clientList.mu.Lock()
	defer kvs.clientList.mu.Unlock()

	if kvs.clientList.list[index] != 0 {
		fmt.Println("[SERVER] Already established first connection for client ", index)
		reply.Value = "ERROR"
		return errors.New("tried to establish a connection that was already established")
	}

	kvs.clientList.list[index] += 1 //Aumento di uno il numero di messaggi ricevuti
	return nil
}
