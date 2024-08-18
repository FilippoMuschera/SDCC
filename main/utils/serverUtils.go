package utils

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"
)

var replicas = os.Getenv("REPLICAS") //Assunzione: per tre repliche, questo valore sarà "3"
var basePort = 8080                  //Porta base: 8080. Le repliche avranno porte successive a questa
var NumberOfReplicas, _ = strconv.Atoi(replicas)

func GetServerPort(index int) string { //Assunzione: gli indici del server partono da 0

	if index > (NumberOfReplicas - 1) { //Se ho tre repliche e sto richiedendo una porta con indice > 2 non è corretto
		fmt.Printf("[ERROR] Requested a port higher than replicas: requested index %d but Replicas = %d\n"+
			"[REPLICAS] = %s", index, NumberOfReplicas, replicas)
		return ""
	}
	return strconv.Itoa(basePort + index) //return come string

}

func GetServerName(index int) string {
	if index > (NumberOfReplicas - 1) { //Se ho tre repliche e sto richiedendo una porta con indice > 2 non è corretto
		fmt.Println("[ERROR] Requested a server higher than replicas")
		return ""
	}

	if os.Getenv("LOCAL") == "1" {
		return "localhost:"
	}

	//Qui poi ci va anche il caso per Docker
	return ""

}

func SendAllAcks(msg Message) {
	fmt.Println("Sending all acks")

	for i := 0; i < NumberOfReplicas; i++ {

		port := GetServerPort(i)
		//TODO delay
		serverName := GetServerName(i)
		addr := serverName + port

		conn, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Failed to connect to server", i)
			os.Exit(1)
		}
		defer conn.Close()

		err = conn.Call("sequential.ReceiveAck", msg, NewResponse())
		if err != nil {
			fmt.Println("Failed to send ack to server", i, "with error: ", err)
		}

	}
}

func SendToAllServer(msg Message) error {
	fmt.Println("Sending to all server")

	for i := 0; i < NumberOfReplicas; i++ {

		port := GetServerPort(i)
		//TODO delay
		serverName := GetServerName(i)
		addr := serverName + port

		conn, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Failed to connect to server", i)
			return err
		}
		defer conn.Close()

		err = conn.Call("sequential.Update", msg, NewResponse())
		if err != nil {
			fmt.Println("Failed to send ack to server", i, "with error: ", err)
			return err
		}

	}
	return nil
}
