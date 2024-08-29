package main

import (
	"SDCC/main/utils"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type VectLogicalClock struct {
	clockVector      []int
	clockVectorMutex sync.Mutex
}

// KVSCausal is a concrete implementation of the KVS interface
type KVSCausal struct {
	index                 int               //indice della replica corrente
	store                 map[string]string //KVS effettivo
	mapMutex              sync.Mutex        //mutex per accedere alla Map
	clientList            ClientList        //Lista dei client per il singolo server
	logicalClock          *VectLogicalClock // clock logico del server
	sendFifoOrderIndex    int               //serve a mantenere il fifo ordering quando il server si invia da solo un'operazione
	sendFifoOrderMutex    sync.Mutex        //mutex per fifo ordering
	receiveFifoOrderIndex int               //serve a mantenere il fifo ordering quando il server riceve un suo messaggio
	receiveFifoOrderMutex sync.Mutex        //mutex per fifo ordering
}

// NewKVSCasual  creates a new instance of KVSCasual
func NewKVSCasual(index int) *KVSCausal {
	numOfReplicas, _ := strconv.Atoi(os.Getenv("REPLICAS")) //numero di server = numero di client
	kvs := &KVSCausal{
		index: index,
		store: make(map[string]string),
		clientList: ClientList{
			list: make([]int, numOfReplicas),
		},
		logicalClock: &VectLogicalClock{
			clockVector: make([]int, numOfReplicas),
		},
	}
	go kvs.printMapAfterExecution()
	return kvs
}

// Update è la funzione dedicata alla ricezione di messaggi che si scambiano i server
func (kvs *KVSCausal) Update(m utils.VMessageNA, resp *utils.Response) error {

	msg := &m

	fmt.Printf("MSG %s ready to wait for exec\n"+
		"OP: %s, (%s, %s), clock vector = %v\n", msg.UUID, msg.OpType, msg.Args.Key, msg.Args.Value, msg.ClockVector)
	kvs.WaitUntilExecutable(msg)
	//Ora si può procedere a eseguire il messaggio
	fmt.Printf("\033[33mMSG %s ready to exec operation\033[0m\n", msg.UUID)
	if msg.ServerIndex == kvs.index { // GET/PUT/DELETE che arriva dal server stesso
		kvs.receiveFifoOrderMutex.Lock()
		defer kvs.receiveFifoOrderMutex.Unlock()
		kvs.mapMutex.Lock()
		defer kvs.mapMutex.Unlock()
		kvs.receiveFifoOrderIndex++
	} else { //Il messaggio arriva da un altro server
		kvs.logicalClock.clockVectorMutex.Lock()
		kvs.mapMutex.Lock()
		defer kvs.logicalClock.clockVectorMutex.Unlock()
		defer kvs.mapMutex.Unlock()
		//Incremento il clock relativo all'evento ricevuto
		kvs.logicalClock.clockVector[msg.ServerIndex]++
	}

	err := kvs.CallRealOperation(msg, resp)
	if err != nil {
		return err
	}

	return nil
}

func (kvs *KVSCausal) checkIfNextFromClient(request utils.Args) bool {
	kvs.clientList.clientListMutex.Lock()
	defer kvs.clientList.clientListMutex.Unlock()
	isNext := kvs.clientList.list[request.ClientIndex]+1 == request.RequestNumber

	if isNext {
		kvs.clientList.list[request.ClientIndex] += 1
	}
	return isNext
}

func (kvs *KVSCausal) WaitUntilExecutable(msg *utils.VMessageNA) {
	/*
			 Questa funzione ha lo scopo di ritornare il controllo alla RPC "originale" (Get, Put, Delete), da cui deve essere
			 invocata, solamente quando il relativo messaggio rispetta tutte le condizioni dell'algoritmo del multicast
			 causalmente ordinato:

			 1. tsm[i] = Vj[i] + 1:
		        m è il messaggio successivo che pj si aspetta da p
			 2. tsm[k] ≤ Vj[k] per ogni k =/= i:
				per ogni altro processo pk, pj ha visto almeno gli stessi messaggi visti da pi

			3. Se è una read devo aspettare l'operazione di write da cui ha una dipendenza causale

			 Se tutte le condizioni sono rispettate, allora la funzione ritornerà il controllo al chiamante, e procederà
			 all'esecuzione della RPC di livello applicativo.

			 La funzione è BLOCCANTE perché ogni richiesta al server è gestita in una goroutine.
	*/

	if kvs.index == msg.ServerIndex { //arriva dal server stesso
		cond0 := make(chan bool)
		go func() {
			for {
				if kvs.isFifoOrdered(msg) {
					cond0 <- true
					return
				}
				time.Sleep(SLEEP_TIME)
			}
		}()
		<-cond0
		fmt.Printf("\033[32mControllo fifoOrder msg interno superato per il messaggio %s\033[0m\n", msg.UUID)

	} else { //Arriva da un server diverso: controlli multicast causalmente ordinato
		cond1 := make(chan bool)
		go func() {
			for {
				if kvs.isNextExpected(msg) {
					cond1 <- true
					return
				}
				time.Sleep(SLEEP_TIME)
			}
		}()
		<-cond1
		fmt.Printf("\033[32mControllo isNextExpected superato per il messaggio %s\033[0m\n", msg.UUID)

		cond2 := make(chan bool)
		go func() {
			for {
				if kvs.haveSeenEnoughMessages(msg) {
					cond2 <- true
					return
				}
				time.Sleep(SLEEP_TIME)
			}
		}()
		<-cond2
		fmt.Printf("\033[32mControllo haveSeenEnoughMessages superato per il messaggio %s\033[0m\n", msg.UUID)
	}

	//Che sia un evento che arriva dal server stesso o da un altro, se è una GET bisogna rispettare la (potenziale)
	//relazione cause-effetto -> deve superare quest'ultimo controllo.

	if msg.OpType == utils.Get {
		cond3 := make(chan bool)
		go func() {
			for {
				if kvs.hasWriteHappened(msg) {
					cond3 <- true
					return
				}
				time.Sleep(SLEEP_TIME)
			}
		}()
		<-cond3
		fmt.Printf("\033[32mControllo haveSeenEnoughMessages superato per il messaggio %s\033[0m\n", msg.UUID)
	}

	//Una volta verificatesi tutte le condizioni, il controllo può tornare alla funzione chiamante e il messaggio
	//può essere passato, di fatto, al livello applicativo.

}

func (kvs *KVSCausal) CallRealOperation(msg *utils.VMessageNA, resp *utils.Response) error {

	switch msg.OpType {
	case utils.Get:
		if msg.ServerIndex != kvs.index {
			return nil
		}
		// Implementazione dell'operazione Get
		value := kvs.store[msg.Args.Key]
		resp.Key = msg.Args.Key
		resp.Value = value
		resp.IsPrintable = true
		fmt.Printf("Get operation completed. Key: %s, Value: %s\n", msg.Args.Key, value)

	case utils.Put:
		// Implementazione dell'operazione Put
		kvs.store[msg.Args.Key] = msg.Args.Value
		fmt.Printf("Put operation completed. Key: %s, Value: %s\n", msg.Args.Key, msg.Args.Value)

	case utils.Delete:
		// Implementazione dell'operazione Delete
		delete(kvs.store, msg.Args.Key) //Se la chiave non c'è ho una no-op ed è il comportamento desiderato
		fmt.Printf("Delete operation completed. Key: %s\n", msg.Args.Key)

	default:
		return fmt.Errorf("unknown operation type: %s", msg.OpType)
	}

	return nil
}

func (kvs *KVSCausal) ExecuteClientRequest(arg utils.Args, resp *utils.Response, op string) error {

	/*
	 Questa funzione non è esposta direttamente verso il client (che utilizzerà le RPC "Get", "Put" e "Delete".
	 A differenza di quanto succede con il caso di consistenza sequenziale, qui non c'è bisogno di gestire
	 gli ack dei messaggi interni
	*/

	//Condizione 0: FIFO ordering per le richieste dai client

	cond0 := make(chan bool)
	go func() {
		for {
			if kvs.checkIfNextFromClient(arg) {
				cond0 <- true
				return
			}
			// Utilizzo di `time.Sleep` per ridurre l'uso della CPU
			time.Sleep(SLEEP_TIME)
		}
	}()

	<-cond0 //aspetto che cond0 sia verificata
	//A questo punto sono sicuro di star processando la richiesta che mi aspettavo dal client.

	kvs.sendFifoOrderMutex.Lock()
	kvs.logicalClock.clockVectorMutex.Lock()
	kvs.logicalClock.clockVector[kvs.index]++ //incremento la componente del clock vettoriale relativa al processo corrente
	//in preparazione alla send
	kvs.sendFifoOrderIndex++

	//Questa copia è necessaria per essere thread safe: Go passa le slice per riferimento. Non creando una copia si creerebbe una
	//race condition: il messaggio andrebbe inviato ai server PRIMA che qualche altro thread locale possa aggiornare il valore del clock.
	//Creando invece la copia non si pone questo problema.
	clockVectorCopy := make([]int, len(kvs.logicalClock.clockVector))
	copy(clockVectorCopy, kvs.logicalClock.clockVector)

	msg := utils.NewVMessageNA(arg, clockVectorCopy, kvs.index, op, kvs.sendFifoOrderIndex)

	kvs.logicalClock.clockVectorMutex.Unlock()
	kvs.sendFifoOrderMutex.Unlock()

	var err error
	if msg.OpType == utils.Get {
		respChannel := make(chan string, 1)
		err = utils.SendGETToAllServerCausal(*msg, respChannel, kvs.index)
		resp.Value = <-respChannel
		resp.Key = msg.Args.Key
		resp.IsPrintable = true

	} else {
		err = utils.SendToAllServerCausal(*msg)

	}

	if err != nil {
		fmt.Println("+++ Error sending to all server:", err)
		return err
	}

	return nil
}

func (kvs *KVSCausal) isNextExpected(msg *utils.VMessageNA) bool {
	/*
		1. tsm[i] = Vj[i] + 1:
		m è il messaggio successivo che pj si aspetta da p
	*/
	kvs.logicalClock.clockVectorMutex.Lock()
	defer kvs.logicalClock.clockVectorMutex.Unlock()

	return msg.ClockVector[msg.ServerIndex] == kvs.logicalClock.clockVector[msg.ServerIndex]+1

}

func (kvs *KVSCausal) haveSeenEnoughMessages(msg *utils.VMessageNA) bool {
	/*
		2. tsm[k] ≤ Vj[k] per ogni k =/= i:
		   per ogni altro processo pk, pj ha visto almeno gli stessi messaggi visti da pi
	*/
	kvs.logicalClock.clockVectorMutex.Lock()
	defer kvs.logicalClock.clockVectorMutex.Unlock()

	for k := 0; k < (utils.NumberOfReplicas); k++ {
		if k != msg.ServerIndex {
			if msg.ClockVector[k] > kvs.logicalClock.clockVector[k] {
				return false
			}
		}
	}
	return true

}

func (kvs *KVSCausal) hasWriteHappened(msg *utils.VMessageNA) bool {
	kvs.mapMutex.Lock()
	defer kvs.mapMutex.Unlock()

	_, isPresent := kvs.store[msg.Args.Key]

	return isPresent
}

func (kvs *KVSCausal) Get(args utils.Args, reply *utils.Response) error {
	// Chiamata al metodo ExecuteClientRequest con l'operazione "Get"
	err := kvs.ExecuteClientRequest(args, reply, utils.Get)
	if err != nil {
		return err
	}
	return nil
}
func (kvs *KVSCausal) Put(args utils.Args, reply *utils.Response) error {
	// Chiamata al metodo ExecuteClientRequest con l'operazione "Put"

	err := kvs.ExecuteClientRequest(args, reply, utils.Put)
	if err != nil {
		return err
	}
	return nil
}

func (kvs *KVSCausal) Delete(args utils.Args, reply *utils.Response) error {
	// Chiamata al metodo ExecuteClientRequest con l'operazione "Delete"
	err := kvs.ExecuteClientRequest(args, reply, utils.Delete)
	if err != nil {
		return err
	}
	return nil
}

func (kvs *KVSCausal) isFifoOrdered(msg *utils.VMessageNA) bool {
	kvs.receiveFifoOrderMutex.Lock()
	defer kvs.receiveFifoOrderMutex.Unlock()

	isNext := kvs.receiveFifoOrderIndex+1 == msg.FifoIndex

	return isNext
}

func (kvs *KVSCausal) printMapAfterExecution() {
	time.Sleep(10 * time.Second)

	// Stampa in maniera formattata il contenuto della map
	kvs.mapMutex.Lock()
	defer kvs.mapMutex.Unlock()

	// Trova la lunghezza massima delle chiavi e dei valori per la formattazione
	maxKeyLen := 3 // La lunghezza minima per "Key"
	maxValLen := 5 // La lunghezza minima per "Value"
	for key, value := range kvs.store {
		if len(key) > maxKeyLen {
			maxKeyLen = len(key)
		}
		if len(value) > maxValLen {
			maxValLen = len(value)
		}
	}

	// Stampa l'intestazione della tabella
	fmt.Println(strings.Repeat("-", maxKeyLen+maxValLen+7))
	fmt.Printf("| %-*s | %-*s |\n", maxKeyLen, "Key", maxValLen, "Value")
	fmt.Println(strings.Repeat("-", maxKeyLen+maxValLen+7))

	// Stampa ogni riga della tabella con key e value
	for key, value := range kvs.store {
		fmt.Printf("| %-*s | %-*s |\n", maxKeyLen, key, maxValLen, value)
	}

	// Stampa la linea di chiusura della tabella
	fmt.Println(strings.Repeat("-", maxKeyLen+maxValLen+7))

}
