package utils

import (
	"fmt"
	"os"
	"strconv"
)

var replicas = os.Getenv("REPLICAS") //Assunzione: per tre repliche, questo valore sarà "3"
var basePort = 8080                  //Porta base: 8080. Le repliche avranno porte successive a questa
var numberOfReplicas, _ = strconv.Atoi(replicas)

func GetServerPort(index int) string { //Assunzione: gli indici del server partono da 0

	if index > (numberOfReplicas - 1) { //Se ho tre repliche e sto richiedendo una porta con indice > 2 non è corretto
		fmt.Printf("[ERROR] Requested a port higher than replicas: requested index %d but Replicas = %d\n"+
			"[REPLICAS] = %s", index, numberOfReplicas, replicas)
		return ""
	}
	return strconv.Itoa(basePort + index) //return come string

}

func GetServerName(index int) string {
	if index > (numberOfReplicas - 1) { //Se ho tre repliche e sto richiedendo una porta con indice > 2 non è corretto
		fmt.Println("[ERROR] Requested a server higher than replicas")
		return ""
	}

	if os.Getenv("LOCAL") == "1" {
		return "localhost:"
	}

	//Qui poi ci va anche il caso per Docker
	return ""

}
