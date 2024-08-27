package utils

import (
	"math/rand"
	"sync"
)

var randomReplicaMutex = sync.Mutex{}
var assignedServer = make([]bool, NumberOfReplicas)

func GetRandomReplica() int {
	randomReplicaMutex.Lock() //thread safe
	defer randomReplicaMutex.Unlock()

	// Lista dei possibili numeri di server non ancora assegnati
	var availableReplicas []int
	for i := 0; i < NumberOfReplicas; i++ {
		if !assignedServer[i] {
			availableReplicas = append(availableReplicas, i)
		}
	}

	// Scegliere casualmente un numero dalla lista disponibile
	if len(availableReplicas) > 0 {
		randomIndex := rand.Intn(len(availableReplicas))
		selectedReplica := availableReplicas[randomIndex]
		assignedServer[selectedReplica] = true
		return selectedReplica
	}

	// In teoria non dovremmo mai arrivare qui se il numero di chiamate è pari a NumberOfReplicas
	return -1
}
