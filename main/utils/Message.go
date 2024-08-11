package utils

import (
	"github.com/google/uuid"
	"slices"
	"sync"
)

type Message struct {
	Args             Args        //Args della richiesta
	ClockValue       int         //clock logico scalare
	Acks             int         //ack ricevuti
	AcksMutex        *sync.Mutex //mutex per modificare il numero di ack
	UUID             uuid.UUID   //unique identifier del messaggio
	ServerIndex      int
	ServerMsgCounter int
}

type MessageQueue struct {
	Queue      []*Message
	QueueMutex sync.Mutex
}

func NewMessage(args Args, clockValue int, serverIndex int, msgCounter int) *Message {
	id := uuid.New()
	return &Message{Args: args, ClockValue: clockValue, Acks: 0, UUID: id, ServerIndex: serverIndex, ServerMsgCounter: msgCounter}
}

func NewMessageQueue() *MessageQueue {
	return &MessageQueue{Queue: make([]*Message, 0)}
}

func (mq *MessageQueue) InsertAndSort(message *Message) {
	mq.QueueMutex.Lock()
	defer mq.QueueMutex.Unlock()
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
