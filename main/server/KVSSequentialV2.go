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

var SLEEP_TIME = 250 * time.Millisecond

type LogicalClock struct {
	clockValue int
	clockMutex sync.Mutex
}

// KVSSequentialV2 is a concrete implementation of the KVS interface
type KVSSequentialV2 struct {
	index        int                 //indice della replica corrente
	store        map[string]string   //KVS effettivo
	mapMutex     sync.Mutex          //mutex per accedere alla Map
	clientList   ClientList          //Lista dei client per il singolo server
	logicalClock *LogicalClock       // clock logico del server
	messageQueue *utils.MessageQueue //coda di messaggi del server
	serverList   ServerList          //struct con contatori di ricezioni/invii per ogni server

}

type ServerList struct {
	SendMsgCounter    int
	sendMsgMutex      sync.Mutex
	ReceiveMsgCounter []int
	receiveMsgMutex   sync.Mutex
}

type ClientList struct {
	list            []int
	clientListMutex sync.Mutex
}

// NewKVSSequentialV2 creates a new instance of KVSSequentialV2.go
func NewKVSSequentialV2(index int) *KVSSequentialV2 {
	numOfReplicas, _ := strconv.Atoi(os.Getenv("REPLICAS")) //numero di server = numero di client
	kvs := &KVSSequentialV2{
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
			SendMsgCounter:    0,
			ReceiveMsgCounter: make([]int, numOfReplicas),
		},
	}
	go kvs.PeriodicCheckForEndKeys()
	return kvs
}

// Update è la funzione dedicata alla ricezione di messaggi che si scambiano i server
func (kvs *KVSSequentialV2) Update(m utils.MessageNA, resp *utils.Response) error {

	msg := &utils.Message{
		Args:             m.Args,
		ClockValue:       m.ClockValue,
		UUID:             m.UUID,
		ServerIndex:      m.ServerIndex,
		ServerMsgCounter: m.ServerMsgCounter,
		OpType:           m.OpType,
	}
	msg.Acks.Store(0)
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

	if msg.OpType == utils.Get {

		if msg.ServerIndex != kvs.index {
			kvs.serverList.ReceiveMsgCounter[msg.ServerIndex] += 1 //conteggio il messaggio come ricevuto dal server
			//Essendo un evento interno di un altro server NON aggiorno volutamente il "mio" clock
			return nil //Evento che non mi serve processare, lo "scarto"
		} else {
			//Se è un "mio" evento interno, simulo la ricezione di tutti gli acke e procedo normalmente
			msg.Acks.Store(int32(utils.NumberOfReplicas))
		}

	}

	//Ora posso effettivamente ricevere il messaggio, inserendolo nella coda
	kvs.logicalClock.clockMutex.Lock()
	msg = kvs.messageQueue.InsertAndSort(msg)

	kvs.serverList.ReceiveMsgCounter[msg.ServerIndex] += 1 //conteggio il messaggio come ricevuto ora

	kvs.UpdateLogicalClockAfterReception(msg)
	kvs.logicalClock.clockMutex.Unlock()

	if msg.Args.Key == utils.EndKey && msg.Args.Value == utils.EndValue {
		//Se è un messaggio di End l'importante è che venga inserito in coda, poi non va
		//realmente processato.
		fmt.Println("\033[1;35mENDOPS FROM SERVER ", msg.ServerIndex, " CORRECTLY PROCESSED\033[0m")
		return nil
	}

	if msg.OpType != utils.Get { //Per le GET (evento interno) non invio ack
		utils.SendAllAcks(m) //Uso la versione del messaggio senza lock per inviare l'ack
	}

	fmt.Printf("MSG %s ready to wait for exec\n", msg.UUID)
	kvs.WaitUntilExecutable(msg)

	err := kvs.CallRealOperation(msg, resp)
	if err != nil {
		return err
	}

	//Dopo aver passato il messaggio al livello applicativo, procedo ad eliminarlo dalla coda
	err = kvs.messageQueue.Pop(msg)
	if err != nil {
		return err
	}

	return nil
}

func (kvs *KVSSequentialV2) checkIfNextFromClient(request utils.Args) bool {
	kvs.clientList.clientListMutex.Lock()
	defer kvs.clientList.clientListMutex.Unlock()
	isNext := kvs.clientList.list[request.ClientIndex]+1 == request.RequestNumber

	if isNext {
		kvs.clientList.list[request.ClientIndex] += 1
	}
	return isNext
}

func (kvs *KVSSequentialV2) WaitUntilExecutable(msg *utils.Message) {
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
	<-cond1
	fmt.Printf("\033[32mControllo sugli ACK superato per il messaggio %s\033[0m\n", msg.UUID)

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
	<-cond2
	fmt.Printf("\033[32mControllo sul clock maggiore superato per il messaggio %s\033[0m\n", msg.UUID)

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

	<-cond3
	fmt.Printf("\033[32mControllo sulla posizione in coda superato per il messaggio %s\033[0m\n", msg.UUID)

	//Una volta verificatesi tutte le condizioni, il controllo può tornare alla funzione chiamante e il messaggio
	//può essere passato, di fatto, al livello applicativo.

}

func (kvs *KVSSequentialV2) checkIfFirstInQueue(msg *utils.Message) bool {
	kvs.messageQueue.QueueMutex.Lock()
	defer kvs.messageQueue.QueueMutex.Unlock()

	// Verifica se il messaggio è il primo nella coda
	firstInQueue := kvs.messageQueue.Queue[0]
	return firstInQueue.UUID == msg.UUID
}

func (kvs *KVSSequentialV2) checkIfNextFromServer(msg *utils.Message) bool {
	kvs.serverList.receiveMsgMutex.Lock()
	defer kvs.serverList.receiveMsgMutex.Unlock()

	isNext := kvs.serverList.ReceiveMsgCounter[msg.ServerIndex]+1 == msg.ServerMsgCounter //controllo se il messaggio che mi è arrivato
	//è effettivamente il prossimo

	/*if isNext {
		kvs.serverList.ReceiveMsgCounter[msg.ServerIndex] += 1 //se lo è allora lo "ricevo", altrimenti resterà in attesa nella Update
	}*/

	return isNext

}

func (kvs *KVSSequentialV2) checkForAllAcks(msg *utils.Message) bool {

	currentAcks := msg.Acks.Load()

	return int(currentAcks) == utils.NumberOfReplicas
}

func (kvs *KVSSequentialV2) checkForHigherClocks(msg *utils.Message) bool {
	//Questo metodo scorre la lista kvs.MessageQueue di Message, e controlla se, per ogni
	//server, è stato ricevuto almeno un messaggio con clock maggiore di quello passato
	// come parametro

	//BUGFIX: C'è un caso particolare a cui fare attenzione: se sono il server 2, per esempio, e in coda ho solo messaggi
	//dal server 0 e dal server 2, questo controllo NON deve ASSOLUTAMENTE passare. Questo perché potrebbe esserci un messaggio
	//"in aria" dal server 1 che precede quello corrente, ma di cui non sono a conoscenza ancora

	kvs.messageQueue.QueueMutex.Lock()
	defer kvs.messageQueue.QueueMutex.Unlock()
	ok := 0

	for serverIndex := 0; serverIndex < utils.NumberOfReplicas; serverIndex++ {
		found := false
		for i := 0; i < len(kvs.messageQueue.Queue); i++ {

			if kvs.messageQueue.Queue[i].ServerIndex == serverIndex &&
				kvs.messageQueue.Queue[i].ClockValue > msg.ClockValue {
				found = true
				ok++
				break
			}

		}
		if !found {
			return false
		}
	}
	return ok == utils.NumberOfReplicas

}

//goland:noinspection GoUnusedParameter
func (kvs *KVSSequentialV2) ReceiveAck(msg utils.MessageNA, resp *utils.Response) error {

	kvs.messageQueue.QueueMutex.Lock()

	var msgToAck *utils.Message = nil

	for _, m := range kvs.messageQueue.Queue {

		if m.UUID == msg.UUID { //Se l'UUID corrisponde a quello di un messaggio che ho in coda -> ho trovato il messaggio relativo all'ack
			msgToAck = m
		}

	}

	if msgToAck != nil { //Il messaggio è presente in coda
		msgToAck.Acks.Add(1)
		kvs.messageQueue.QueueMutex.Unlock()

	} else { //Caso in cui io riceva l'ack di un messaggio non ancora ricevuto
		newMsg := &utils.Message{
			Args:             msg.Args,
			ClockValue:       msg.ClockValue,
			UUID:             msg.UUID,
			ServerIndex:      msg.ServerIndex,
			ServerMsgCounter: msg.ServerMsgCounter,
			OpType:           msg.OpType,
		}
		newMsg.Acks.Store(1)

		_ = kvs.messageQueue.InsertAndSort(newMsg, true)
		kvs.messageQueue.QueueMutex.Unlock()

	}

	return nil
}

func (kvs *KVSSequentialV2) CallRealOperation(msg *utils.Message, resp *utils.Response) error {
	kvs.mapMutex.Lock()
	defer kvs.mapMutex.Unlock()

	switch msg.OpType {
	case utils.Get:
		// Implementazione dell'operazione Get
		value, ok := kvs.store[msg.Args.Key]
		if !ok {
			resp.Key = msg.Args.Key
			resp.Value = utils.KeyNotFound //Non è un vero e proprio errore, può succedere che un client richieda una risorsa che è stata eliminata da altri
			resp.IsPrintable = true
			fmt.Printf("Get operation completed. Key: %s, Value: %s\n", resp.Key, resp.Value)
			break
		}
		resp.Key = msg.Args.Key
		resp.Value = value
		resp.IsPrintable = true
		fmt.Printf("Get operation completed. Key: %s, Value: %s\n", msg.Args.Key, value)

	case utils.Put:
		// Implementazione dell'operazione Put
		if msg.Args.Key == utils.EndKey && msg.Args.Value == utils.EndValue {
			return nil //Se è il messaggio fittizio di End, non eseguire realmente la PUT
		}
		kvs.store[msg.Args.Key] = msg.Args.Value
		fmt.Printf("Put operation completed. Key: %s, Value: %s\n", msg.Args.Key, msg.Args.Value)

	case utils.Delete:
		// Implementazione dell'operazione Delete
		/*_, ok := kvs.store[msg.Args.Key]
		if !ok {
			return fmt.Errorf("error during Delete operation: key '%s' not found", msg.Args.Key)
		}*/
		delete(kvs.store, msg.Args.Key) //Se la chiave non c'è ho una no-op ed è il comportamento desiderato
		fmt.Printf("Delete operation completed. Key: %s\n", msg.Args.Key)

	default:
		return fmt.Errorf("unknown operation type: %s", msg.OpType)
	}

	return nil
}

func (kvs *KVSSequentialV2) ExecuteClientRequest(arg utils.Args, resp *utils.Response, op string) error {

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

	kvs.logicalClock.clockMutex.Lock()
	kvs.serverList.sendMsgMutex.Lock()
	kvs.serverList.SendMsgCounter += 1

	sendCounter := kvs.serverList.SendMsgCounter
	clockValue := kvs.logicalClock.clockValue
	msg := utils.NewMessageNA(arg, clockValue, kvs.index, sendCounter, op)

	kvs.logicalClock.clockValue = clockValue + 1
	kvs.logicalClock.clockMutex.Unlock()

	kvs.serverList.sendMsgMutex.Unlock()

	var err error
	if msg.OpType == utils.Get {
		respChannel := make(chan string, 1)
		err = utils.SendGETToAllServer(*msg, respChannel, kvs.index)
		resp.Value = <-respChannel
		resp.Key = msg.Args.Key
		resp.IsPrintable = true

	} else {
		err = utils.SendToAllServer(*msg)

	}

	if err != nil {
		fmt.Println("+++ Error sending to all server:", err)
		return err
	}

	return nil
}

func (kvs *KVSSequentialV2) UpdateLogicalClockAfterReception(m *utils.Message) {

	if kvs.logicalClock.clockValue < m.ClockValue { //Il messaggio ha un clock maggiore di quello corrente, allora aggiorno
		kvs.logicalClock.clockValue = m.ClockValue
	}
}

func (kvs *KVSSequentialV2) Get(args utils.Args, reply *utils.Response) error {
	// Chiamata al metodo ExecuteClientRequest con l'operazione "Get"
	err := kvs.ExecuteClientRequest(args, reply, utils.Get)
	if err != nil {
		return err
	}
	return nil
}
func (kvs *KVSSequentialV2) Put(args utils.Args, reply *utils.Response) error {
	// Chiamata al metodo ExecuteClientRequest con l'operazione "Put"

	err := kvs.ExecuteClientRequest(args, reply, utils.Put)
	if err != nil {
		return err
	}
	return nil
}

func (kvs *KVSSequentialV2) Delete(args utils.Args, reply *utils.Response) error {
	// Chiamata al metodo ExecuteClientRequest con l'operazione "Delete"
	err := kvs.ExecuteClientRequest(args, reply, utils.Delete)
	if err != nil {
		return err
	}
	return nil
}

func (kvs *KVSSequentialV2) PeriodicCheckForEndKeys() {
	for {
		kvs.checkForEndKeys()
		time.Sleep(3 * time.Second)
	}
}

func (kvs *KVSSequentialV2) checkForEndKeys() {
	kvs.messageQueue.QueueMutex.Lock()
	defer kvs.messageQueue.QueueMutex.Unlock()

	// Verifica se la lunghezza della coda è uguale a numberOfReplicas
	if len(kvs.messageQueue.Queue) != utils.NumberOfReplicas {
		return
	}

	// Controlla che tutti i messaggi nella coda abbiano Args.Key == utils.EndKey e Args.Value == utils.EndValue
	for _, msg := range kvs.messageQueue.Queue {
		if msg.Args.Key != utils.EndKey || msg.Args.Value != utils.EndValue {
			return
		}
	}

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

	// Svuota la coda dagli END MESSAGE
	kvs.messageQueue.Queue = kvs.messageQueue.Queue[:0]

}
