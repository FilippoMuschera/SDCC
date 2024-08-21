package utils

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"sync"
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
	} else if os.Getenv("DOCKER") == "1" {
		return "server" + strconv.Itoa(index) + ":"
	}

	//Qui poi ci va anche il caso per Docker
	return ""

}

func SendAllAcks(msg MessageNA) {
	fmt.Println("Sending all acks")

	var wg sync.WaitGroup
	sent := 0
	wg.Add(NumberOfReplicas)

	for i := 0; i < NumberOfReplicas; i++ {

		port := GetServerPort(i)
		NetworkDelay()
		serverName := GetServerName(i)
		addr := serverName + port

		conn, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Failed to connect to server", i)
			os.Exit(1)
		}

		go func() {
			err = conn.Call("sequential.ReceiveAck", msg, NewResponse())
			if err != nil {
				fmt.Println("Failed to send ack to server", i, "with error: ", err)
				return
			}
			fmt.Printf("\033[32;1mSent ACK to server %d at address %s [UUID %s]\033[0m\n", i, addr, msg.UUID)
			sent += 1
			err := conn.Close()
			if err != nil {
				fmt.Println("Error closing ack connection: ", err)
			}
			wg.Done()

		}()

	}
	wg.Wait()
	if sent != NumberOfReplicas {
		fmt.Printf("[ERROR] Sent %d acks instead of %d", sent, NumberOfReplicas)
		os.Exit(1)
	}
}

func SendToAllServer(msg MessageNA) error {
	fmt.Println("Sending to all server")

	var wg sync.WaitGroup
	wg.Add(NumberOfReplicas)

	for i := 0; i < NumberOfReplicas; i++ {

		port := GetServerPort(i)
		NetworkDelay()
		serverName := GetServerName(i)
		addr := serverName + port

		conn, err := rpc.Dial("tcp", addr)
		if err != nil {
			fmt.Println("Failed to connect to server", i)
			return err
		}

		go func() {
			err := conn.Call("sequential.Update", msg, NewResponse())
			if err != nil {
				fmt.Printf("\033[31mFailed to send msg to server %d with error: %s\033[0m\n", i, err)
			}
			err2 := conn.Close()
			if err != nil {
				fmt.Println("WARNING:::: Error closing ack connection: ", err2)
			}
			wg.Done()
		}()

		fmt.Printf("\033[93mCalled Update on address %s\033[0m\n", addr)

	}
	wg.Wait()
	return nil
}
