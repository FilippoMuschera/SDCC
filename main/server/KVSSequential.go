package main

import (
	"SDCC/main/utils"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

var SLEEP_TIME = 10 * time.Millisecond

type LogicalClock struct {
	clockValue int
	clockMutex sync.Mutex
}

// KVSSequential is a concrete implementation of the KVS interface
type KVSSequential struct {
	index        int                 //indice della replica corrente
	store        map[string]string   //KVS effettivo
	mapMutex     sync.Mutex          //mutex per accedere alla Map
	clientList   ClientList          //Lista dei client per il singolo server
	logicalClock *LogicalClock       // clock logico del server
	messageQueue *utils.MessageQueue //coda di messaggi del server
	serverList   ServerList          //struct con contatori di ricezioni/invii per ogni server
}

type ServerList struct {
	SendMsgCounter    []int
	sendMsgMutex      sync.Mutex
	ReceiveMsgCounter []int
	receiveMsgMutex   sync.Mutex
}

type ClientList struct {
	list            []int
	clientListMutex sync.Mutex
}

// NewKVSSequential creates a new instance of KVSSequential
func NewKVSSequential(index int) *KVSSequential {
	numOfReplicas, _ := strconv.Atoi(os.Getenv("REPLICAS")) //numero di server = numero di client
	return &KVSSequential{
		index: index,
		store: make(map[string]string),
		clientList: ClientList{
			list: make([]int, numOfReplicas),
		},
		logicalClock: &LogicalClock{
			clockValue: 0,
		},
		messageQueue: utils.NewMessageQueue(),
		serverList: ServerList{
			SendMsgCounter:    make([]int, numOfReplicas),
			ReceiveMsgCounter: make([]int, numOfReplicas),
		},
	}
}

// Update è la funzione dedicata alla ricezione di messaggi che si scambiano i server
func (kvs *KVSSequential) Update(msg utils.Message, resp *utils.Response) error {

	//Condizione 0: FIFO ordering per le richieste
	cond0 := make(chan bool)
	go func() {
		for {
			if kvs.checkIfNextFromServer(msg) {
				cond0 <- true
				return
			}
			// Utilizzo di `time.Sleep` per ridurre l'uso della CPU
			time.Sleep(SLEEP_TIME)
		}
	}()

	<-cond0 //aspetto che cond0 sia verificata

	//Ora posso effettivamente ricevere il messaggio, inserendolo nella coda
	kvs.messageQueue.InsertAndSort(&msg)

	kvs.UpdateLogicalClockAfterReception(&msg) //TODO

	utils.SendAllAcks(msg, kvs.index)

	kvs.WaitUntilExecutable(msg)

	err := kvs.CallRealOperation(msg, resp)
	if err != nil {
		return err
	}

	//Dopo aver passato il messaggio al livello applicativo, procedo ad eliminarlo dalla coda
	err = kvs.messageQueue.Pop(&msg)
	if err != nil {
		return err
	}

	return nil
}

func (kvs *KVSSequential) checkIfNextFromClient(request utils.Args) bool {
	kvs.clientList.clientListMutex.Lock()
	defer kvs.clientList.clientListMutex.Unlock()
	isNext := kvs.clientList.list[request.ClientIndex]+1 == request.RequestNumber
	//TODO controllare se aggiornare qui il numero di ricezioni o se deve farlo il chiamante
	if isNext {
		kvs.clientList.list[request.ClientIndex] += 1
	}
	fmt.Println("[SERVER] checkIfNextFromClient", request.RequestNumber, isNext)
	return isNext
}

func (kvs *KVSSequential) WaitUntilExecutable(msg utils.Message) {
	/*
	 Questa funzione ha lo scopo di ritornare il controllo alla RPC "originale" (Get, Put, Delete), da cui deve essere
	 invocata, solamente quando il relativo messaggio rispetta tutte le condizioni dell'algoritmo del multicast
	 totalmente ordinato:

	 1. È stato ricevuto l'ack da tutti gli altri server
	 2. Per ogni altro server c'è un messaggio con clock logico scalare maggiore di questo.

	 Se tutte le condizioni sono rispettate, allora la funzione ritornerà il controllo al chiamante, e procederà
	 all'esecuzione della RPC di livello applicativo.

	 La funzione è BLOCCANTE perché ogni richiesta al server è gestita in una goroutine.
	*/

	fmt.Println("Entering WaitUntilExecutable") //debug

	cond1 := make(chan bool)
	go func() {
		for {
			if kvs.checkForAllAcks(msg) {
				cond1 <- true
				return
			}
			time.Sleep(SLEEP_TIME)
		}
	}()

	cond2 := make(chan bool)
	go func() {
		for {
			if kvs.checkForHigherClocks(msg) {
				cond2 <- true
				return
			}
			time.Sleep(SLEEP_TIME)
		}
	}()

	cond3 := make(chan bool)
	go func() {
		for {
			if kvs.checkIfFirstInQueue(msg) {
				cond3 <- true
				return
			}
			time.Sleep(SLEEP_TIME)
		}
	}()

	<-cond1
	<-cond2
	<-cond3

	//Una volta verificatesi tutte e 4 le condizioni, il controllo puà tornare alla funzione chiamante e il messaggio
	//può essere passato, di fatto, al livello applicativo.

}

func (kvs *KVSSequential) checkIfFirstInQueue(msg utils.Message) bool {
	kvs.messageQueue.QueueMutex.Lock()
	defer kvs.messageQueue.QueueMutex.Unlock()

	firstInQueue := kvs.messageQueue.Queue[0]
	return firstInQueue.UUID == msg.UUID

}

func (kvs *KVSSequential) checkIfNextFromServer(msg utils.Message) bool {
	kvs.serverList.receiveMsgMutex.Lock()
	defer kvs.serverList.receiveMsgMutex.Unlock()

	isNext := kvs.serverList.ReceiveMsgCounter[msg.ServerIndex]+1 == msg.ServerMsgCounter //controllo se il messaggio che mi è arrivato
	//è effettivamente il prossimo

	if isNext {
		kvs.serverList.ReceiveMsgCounter[msg.ServerIndex] += 1 //se lo è allora lo "ricevo", altrimenti resterà in attesa nella Update
	}

	return isNext

}

func (kvs *KVSSequential) checkForAllAcks(msg utils.Message) bool {
	msg.AcksMutex.Lock()
	defer msg.AcksMutex.Unlock()

	return msg.Acks == utils.NumberOfReplicas
}

func (kvs *KVSSequential) checkForHigherClocks(msg utils.Message) bool {
	//Questo metodo scorre la lista kvs.MessageQueue di Message, e controlla se, per ogni
	//server, è stato ricevuto almeno un messaggio con clock maggiore di quello passato
	// come parametro

	kvs.messageQueue.QueueMutex.Lock()
	defer kvs.messageQueue.QueueMutex.Unlock()

	for serverIndex := 0; serverIndex < utils.NumberOfReplicas; serverIndex++ {
		found := false
		for i := 0; i < len(kvs.messageQueue.Queue); i++ {

			if kvs.messageQueue.Queue[i].ServerIndex == serverIndex &&
				kvs.messageQueue.Queue[i].ClockValue > msg.ClockValue {
				found = true
				break
			}

		}
		if !found {
			return false
		}
	}
	return true

}

func (kvs *KVSSequential) ReceiveAck(msg utils.Message, resp *utils.Response) error {

	kvs.messageQueue.QueueMutex.Lock()
	defer kvs.messageQueue.QueueMutex.Unlock()

	var msgToAck *utils.Message = nil

	for _, m := range kvs.messageQueue.Queue {

		if m.UUID == msg.UUID { //Se l'UUID corrisponde a quello di un messaggio che ho in coda -> ho trovato il messaggio relativo all'ack
			msgToAck = m
		}

	}

	if msgToAck != nil { //Il messaggio è presente in coda
		msgToAck.AcksMutex.Lock()
		msgToAck.Acks += 1
		msgToAck.AcksMutex.Unlock()
	} else { //Caso in cui io riceva l'ack di un messaggio non ancora ricevuto
		msg.AcksMutex.Lock()
		msg.Acks += 1 //Andrò ad inserire un nuovo messaggio e tengo conto del fatto che ha anche un ack già ricevuto
		msg.AcksMutex.Unlock()
		kvs.messageQueue.InsertAndSort(&msg, true)
	}

	return nil
}

func (kvs *KVSSequential) CallRealOperation(msg utils.Message, resp *utils.Response) error {
	kvs.mapMutex.Lock()
	defer kvs.mapMutex.Unlock()

	switch msg.OpType {
	case utils.Get:
		// Implementazione dell'operazione Get
		value, ok := kvs.store[msg.Args.Key]
		if !ok {
			return fmt.Errorf("error during Get operation: key '%s' not found", msg.Args.Key)
		}
		resp.Value = value
		resp.IsPrintable = true
		fmt.Printf("Get operation completed. Key: %s, Value: %s\n", msg.Args.Key, value)

	case utils.Put:
		// Implementazione dell'operazione Put
		kvs.store[msg.Args.Key] = msg.Args.Value
		fmt.Printf("Put operation completed. Key: %s, Value: %s\n", msg.Args.Key, msg.Args.Value)

	case utils.Delete:
		// Implementazione dell'operazione Delete
		_, ok := kvs.store[msg.Args.Key]
		if !ok {
			return fmt.Errorf("error during Delete operation: key '%s' not found", msg.Args.Key)
		}
		delete(kvs.store, msg.Args.Key)
		fmt.Printf("Delete operation completed. Key: %s\n", msg.Args.Key)

	default:
		return fmt.Errorf("unknown operation type: %s", msg.OpType)
	}

	return nil
}

func (kvs *KVSSequential) ExecuteClientRequest(msg utils.Message, resp *utils.Response) error {

	/*
			 Questa funzione non è esposta direttamente verso il client (che utilizzerà le RPC "Get", "Put" e "Delete".
			 Nel caso di una GET questa funzione provvede a generare un evento INTERNO: verrà dunque creato un messaggio ed inserito
			 solamente nella coda locale, senza essere inoltrato con l'Update a tutti gli altri server (proprio perché è un evento interno).

			 Per ovviare al fatto che è un messaggio di un evento interno, andrà creato un messaggio i cui ACK risultano già tutti
			 pervenuti (in maniera "fittizia"). In questo modo verrà eseguito quando sarà il primo della coda e per ogni altro processo
			 p_k avrò ricevuto un messaggio con clock maggiore del suo.

			Nel caso di una PUT o di una DELETE invece, si andrà a generare un messaggio e lo si invierà a tutti i server (compreso
		    quello corrente) tramite la RPC Update.

			Prima di poter eseguire queste operazioni bisogna accertarsi che il messaggio che si è ricevuto sia quello che ci si
			aspettava dal client: bisogna rispettare l'ordinamento FIFO delle operazioni.

			Inoltre, all'atto di creazione di un messaggio BISOGNA PRENDERE IL LOCK SUL CLOCK LOGICO, INCREMENTARLO, ASSEGNARE IL NUOVO
			CLOCK AL MESSAGGIO, E POI RILASCIARLO!

			Nel messaggio, infine, bisogna anche considerare il fatto che bisognerà inserire il numero del messaggio per l'ordinamento
			FIFO, in modo che i server riceventi possano eventualmente bufferizzare il messaggio in fase di ricezione. Questa operazione
			dovrà fare uso dei relativi mutex delle strutture dati associate.
	*/

}
