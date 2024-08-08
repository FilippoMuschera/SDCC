package main

import (
	"SDCC/main/utils"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

var SLEEP_TIME = 10 * time.Millisecond

// KVSSequential is a concrete implementation of the KVS interface
type KVSSequential struct {
	store          map[string]string
	mapMutex       sync.Mutex      //mutex per accedere alla Map
	clientList     ClientList      //Lista dei client per il singolo server
	requestBuffers []RequestBuffer //buffer per le richieste dei client. Un buffer per ogni server
}

type ClientList struct {
	list            []int
	clientListMutex sync.Mutex
}

type RequestBuffer struct {
	buffer             []utils.Args
	requestBufferMutex sync.Mutex
}

// NewKVSSequential creates a new instance of KVSSequential
func NewKVSSequential() *KVSSequential {
	numOfClients, _ := strconv.Atoi(os.Getenv("REPLICAS"))
	return &KVSSequential{
		store: make(map[string]string),
		clientList: ClientList{
			list: make([]int, numOfClients),
		},
		requestBuffers: make([]RequestBuffer, numOfClients),
	}
}

// Get retrieves a value from the key-value store
func (kvs *KVSSequential) Get(args utils.Args, reply *utils.Response) error {
	fmt.Println("Entering GET operation")
	kvs.WaitUntilExecutable(args)
	kvs.mapMutex.Lock()
	defer kvs.mapMutex.Unlock()
	fmt.Println("Serving GET operation, lock acquired")

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
	fmt.Println("Entering PUT operation")
	kvs.WaitUntilExecutable(args)
	kvs.mapMutex.Lock()
	fmt.Println("Serving PUT operation, lock acquired")
	defer kvs.mapMutex.Unlock()

	kvs.store[args.Key] = args.Value
	reply.Value = ""
	return nil
}

// Delete removes a key from the key-value store
func (kvs *KVSSequential) Delete(args utils.Args, reply *utils.Response) error {
	kvs.WaitUntilExecutable(args)
	kvs.mapMutex.Lock()
	defer kvs.mapMutex.Unlock()

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
	kvs.clientList.clientListMutex.Lock()
	defer kvs.clientList.clientListMutex.Unlock()

	if kvs.clientList.list[args.ClientIndex] != 0 || args.RequestNumber != 1 {
		fmt.Println("[SERVER] Already established first connection for client ", args.ClientIndex)
		reply.Value = "ERROR"
		return errors.New("tried to establish a connection that was already established")
	}

	kvs.clientList.list[args.ClientIndex] += 1 //Aumento di uno il numero di messaggi ricevuti
	return nil
}

func (kvs *KVSSequential) checkIfNextFromClient(request utils.Args) bool {
	kvs.clientList.clientListMutex.Lock()
	defer kvs.clientList.clientListMutex.Unlock()
	isNext := kvs.clientList.list[request.ClientIndex]+1 == request.RequestNumber
	if isNext {
		kvs.clientList.list[request.ClientIndex] += 1
	}
	fmt.Println("[SERVER] checkIfNextFromClient", request.RequestNumber, isNext)
	return isNext
}

func (kvs *KVSSequential) WaitUntilExecutable(request utils.Args) {
	/*
	 Questa funzione ha lo scopo di ritornare il controllo alla RPC "originale" (Get, Put, Delete), da cui deve essere
	 invocata, solamente quando il relativo messaggio rispetta tutte le condizioni dell'algoritmo del multicast
	 totalmente ordinato:
	 0. È il messaggio "expected" dal client (il prossimo nella sequenza FIFO)
	 1. È il primo nella coda dei messaggi
	 2. È stato ricevuto l'ack da tutti gli altri server
	 3. Per ogni altro server c'è un messaggio con clock logico scalare maggiore di questo.

	 Se tutte le condizioni sono rispettate, allora la funzione ritornerà il controllo al chiamante, e procederà
	 all'esecuzione della RPC di livello applicativo.

	 La funzione è BLOCCANTE perché ogni richiesta al server è gestita in una goroutine.
	*/

	fmt.Println("Entering WaitUntilExecutable") //debug
	//Condizione 0: FIFO ordering per le richieste
	cond0 := make(chan bool)
	go func() {
		for {
			if kvs.checkIfNextFromClient(request) {
				cond0 <- true
				return
			}
			// Utilizzo di `time.Sleep` per ridurre l'uso della CPU
			time.Sleep(SLEEP_TIME)
		}
	}()

	<-cond0 //Aspetta la condizione 0

}
