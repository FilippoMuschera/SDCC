package utils

import (
	"math/rand"
	"time"
)

var r *rand.Rand
var SEED = int64(123456)

func init() {
	r = rand.New(rand.NewSource(SEED))
}

func NetworkDelay() {

	// Genera un tempo randomico tra 1 e 300 millisecondi
	sleepTime := time.Duration(r.Intn(300)+1) * time.Millisecond

	// Effettua la sleep
	time.Sleep(sleepTime)
}
