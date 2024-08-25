package utils

import (
	"math"
	"math/rand"
	"time"
)

/*
 * Questa funzione ha lo scopo di modellare il ritardo di rete riscontrato dai messaggi e dagli ack inviati tra i server.
 * Allo scopo di rendere questo ritardo quanto più realistico possibile si è implementato tramite l'utilizzo di una
 * distribuzione log-normale.
 * Fonti:
 * - https://www.researchgate.net/publication/259339669_On_the_Log-Normal_Distribution_of_Network_Traffic
 * - https://docs.hoverfly.io/en/latest/pages/tutorials/basic/delays/lognormal/lognormal.html
 */

// Define the parameters for the log-normal distribution.
var (
	MU    = 4.488 // Mean of the underlying normal distribution
	SIGMA = 0.5   // Standard deviation of the underlying normal distribution
)

/*
https://www.wolframalpha.com/input?i=lognormal%284.488%2C+0.5%29
Dati della distribuzione presi in modo che la media sia circa 100 ms, e la varianza faccia oscillare i valori ottenuti
tra la decina di ms (circa) e un valore massimo troncato a 500 ms
*/

func NetworkDelay2() {
	// Generate a log-normal distributed random number.
	// rand.NormFloat64() generates a normally distributed float64 value with mean 0 and stddev 1.
	delay := rand.NormFloat64()*SIGMA + MU
	delayDuration := time.Duration(math.Exp(delay)) * time.Millisecond
	if delayDuration > 500*time.Millisecond {
		delayDuration = 500 * time.Millisecond
	}

	time.Sleep(delayDuration)
}

func NetworkDelay() {

	// Genera un tempo randomico tra 10 e 500 millisecondi
	sleepTime := time.Duration(rand.Intn(490)+10) * time.Millisecond

	// Effettua la sleep
	time.Sleep(sleepTime)
}
