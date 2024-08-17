package utils

import (
	"fmt"
	"github.com/google/uuid"
	"slices"
	"sync"
)

const (
	Get    = "Get"
	Put    = "Put"
	Delete = "Delete"
)

type Message struct {
	Args             Args        //Args della richiesta
	ClockValue       int         //clock logico scalare
	Acks             int         //ack ricevuti
	AcksMutex        *sync.Mutex //mutex per modificare il numero di ack
	UUID             uuid.UUID   //unique identifier del messaggio
	ServerIndex      int
	ServerMsgCounter int
	OpType           string
}

type MessageQueue struct {
	Queue      []*Message
	QueueMutex sync.Mutex
}

func NewMessage(args Args, clockValue int, serverIndex int, msgCounter int, opType string) *Message {
	id := uuid.New()
	return &Message{Args: args, ClockValue: clockValue, Acks: 0, UUID: id, ServerIndex: serverIndex, ServerMsgCounter: msgCounter, OpType: opType}
}

func NewMessageQueue() *MessageQueue {
	return &MessageQueue{Queue: make([]*Message, 0)}
}

func (mq *MessageQueue) InsertAndSort(message *Message, alreadyLocked ...bool) {
	if len(alreadyLocked) == 0 {
		mq.QueueMutex.Lock()
		defer mq.QueueMutex.Unlock()
	}

	// Controlla se il messaggio è già presente nella coda. Si può verificare se ho già ricevuto l'ack del messaggio
	//e ora sto ricevendo il messaggio "vero e proprio". In questo caso nella coda ho già il messaggio e allora non devo
	//fare nulla
	for _, m := range mq.Queue {
		if m.UUID == message.UUID {
			// Messaggio duplicato trovato, esci dalla funzione senza fare nulla
			return
		}
	}

	mq.Queue = append(mq.Queue, message)
	//Ora si esegue il sorting della coda: la coda viene ordinata per valore di clock logico "ClockValue".
	//A parità di ClockValue, si ordina sulla base dello unique identifier, favorendo quella con lo UUID
	//che viene prima in ordine alfabetico

	// Esegui l'ordinamento della coda usando slices.SortStableFunc
	slices.SortStableFunc(mq.Queue, func(a, b *Message) int {
		// Prima ordina per ClockValue
		if a.ClockValue < b.ClockValue {
			return -1
		}
		if a.ClockValue > b.ClockValue {
			return 1
		}
		// Se ClockValue è uguale, ordina per UUID (in ordine alfabetico)
		if a.UUID.String() < b.UUID.String() {
			return -1
		}
		if a.UUID.String() > b.UUID.String() {
			return 1
		}
		return 0
	})
}

func (mq *MessageQueue) Pop(m *Message) error {
	mq.QueueMutex.Lock()
	defer mq.QueueMutex.Unlock()

	//Controllo che "m" sia il primo messaggio nella coda e poi lo elimino da questa

	// Verifica che la coda non sia vuota
	if len(mq.Queue) == 0 {
		return fmt.Errorf("message queue is empty")
	}

	// Controlla che il primo messaggio nella coda sia quello specificato
	if mq.Queue[0].UUID != m.UUID {
		return fmt.Errorf("the provided message is not the first in the queue")
	}

	// Rimuove il primo messaggio dalla coda
	mq.Queue = mq.Queue[1:]

	return nil
}
