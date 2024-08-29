package utils

import (
	"fmt"
	"github.com/google/uuid"
	"slices"
	"sync"
	"sync/atomic"
)

const (
	Get         = "Get"
	Put         = "Put"
	Delete      = "Delete"
	EndKey      = "EndKey"
	EndValue    = "EndValue"
	KeyNotFound = "KeyNotFound"
)

type Message struct {
	Args             Args         //Args della richiesta
	ClockValue       int          //clock logico scalare
	Acks             atomic.Int32 //ack ricevuti
	UUID             uuid.UUID    //unique identifier del messaggio
	ServerIndex      int
	ServerMsgCounter int
	OpType           string
}

type MessageNA struct {
	Args             Args      //Args della richiesta
	ClockValue       int       //clock logico scalare
	UUID             uuid.UUID //unique identifier del messaggio
	ServerIndex      int
	ServerMsgCounter int
	OpType           string
}

func NewMessageNA(args Args, clockValue int, serverIndex int, msgCounter int, opType string) *MessageNA {
	id := uuid.New()
	msg := &MessageNA{Args: args, ClockValue: clockValue, UUID: id, ServerIndex: serverIndex, ServerMsgCounter: msgCounter, OpType: opType}
	return msg

}

type MessageQueue struct {
	Queue      []*Message
	QueueMutex sync.Mutex
}

func NewMessageQueue() *MessageQueue {
	return &MessageQueue{Queue: make([]*Message, 0)}
}

func (mq *MessageQueue) InsertAndSort(message *Message, alreadyLocked ...bool) *Message {
	if len(alreadyLocked) == 0 {
		mq.QueueMutex.Lock()
		defer mq.QueueMutex.Unlock()
	}

	// Controlla se il messaggio è già presente nella coda. Si può verificare se ho già ricevuto l'ack del messaggio
	//e ora sto ricevendo il messaggio "vero e proprio". In questo caso nella coda ho già il messaggio e allora non devo
	//fare nulla
	for _, m := range mq.Queue {
		if m.UUID == message.UUID {
			// Messaggio duplicato trovato, esci dalla funzione e ritorna l'indirizzo di quello già presente
			return m
		}
	}

	mq.Queue = append(mq.Queue, message)
	//Ora si esegue il sorting della coda: la coda viene ordinata per valore di clock logico "ClockVector".
	//A parità di ClockVector, si ordina sulla base dello unique identifier, favorendo quella con lo UUID
	//che viene prima in ordine alfabetico

	// Esegui l'ordinamento della coda usando slices.SortStableFunc
	slices.SortStableFunc(mq.Queue, func(a, b *Message) int {
		// Prima ordina per ClockVector
		if a.ClockValue < b.ClockValue {
			return -1
		}
		if a.ClockValue > b.ClockValue {
			return 1
		}
		// Se ClockVector è uguale, ordina per UUID (in ordine alfabetico)
		if a.UUID.String() < b.UUID.String() {
			return -1
		}
		if a.UUID.String() > b.UUID.String() {
			return 1
		}
		return 0
	})

	// Stampa l'attuale composizione della coda di messaggi in giallo scuro
	fmt.Print("\033[33mCurrent message queue composition:\033[0m\n") // Giallo scuro per intestazione

	for _, m := range mq.Queue {
		fmt.Printf("UUID: %s, OpType: %s, Acks: %d, originatingServer: %d, CLOCK: %d\n", m.UUID, m.OpType, m.Acks.Load(), m.ServerIndex, m.ClockValue)
	}

	// Stampa l'UUID del messaggio che stiamo controllando
	fmt.Printf("\033[33mChecking message UUID: %s\033[0m\n", message.UUID) // Giallo scuro per UUID del messaggio in controllo*/

	return message
}

func (mq *MessageQueue) Pop(uuid uuid.UUID) error {
	mq.QueueMutex.Lock()
	defer mq.QueueMutex.Unlock()

	//Controllo che "m" sia il primo messaggio nella coda e poi lo elimino da questa

	// Verifica che la coda non sia vuota
	if len(mq.Queue) == 0 {
		return fmt.Errorf("message queue is empty")
	}

	// Controlla che il primo messaggio nella coda sia quello specificato
	if mq.Queue[0].UUID != uuid {
		fmt.Println("\033[31mERRORE NELLA POP: IL MSG NON È IL PRIMO IN CODA\n"+
			"I am ", uuid, " first is ", mq.Queue[0].UUID, "\n\033[0m")
		return fmt.Errorf("the provided message is not the first in the queue")
	}

	// Rimuove il primo messaggio dalla coda
	mq.Queue = mq.Queue[1:]

	// Stampa l'attuale composizione della coda di messaggi in giallo scuro
	fmt.Print("\033[33mCurrent message queue composition after POP:\033[0m\n") // Giallo scuro per intestazione

	for _, m := range mq.Queue {
		fmt.Printf("UUID: %s, OpType: %s, Acks: %d, originatingServer: %d, CLOCK: %d\n", m.UUID, m.OpType, m.Acks.Load(), m.ServerIndex, m.ClockValue)
	}

	// Stampa l'UUID del messaggio che stiamo controllando
	fmt.Printf("\033[33mChecking message UUID: %s\033[0m\n", uuid) // Giallo scuro per UUID del messaggio in controllo*/

	return nil
}

type VMessageNA struct {
	Args        Args      //Args della richiesta
	ClockVector []int     //clock logico vettoriale
	UUID        uuid.UUID //unique identifier del messaggio
	ServerIndex int       //server d'origine
	OpType      string    //nome operazione
	FifoIndex   int       //indice per ordinamento fifo operazioni dello stesso processo
}

func NewVMessageNA(args Args, clockValue []int, serverIndex int, opType string, fifoIndex int) *VMessageNA {
	id := uuid.New()
	msg := &VMessageNA{Args: args, ClockVector: clockValue, UUID: id, ServerIndex: serverIndex, OpType: opType, FifoIndex: fifoIndex}

	return msg

}
