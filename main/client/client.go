package main

import (
	"SDCC/main/utils"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
)

func main() {
	// Check command line arguments
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run client.go <client_index>")
		os.Exit(1)
	}
	index, err := strconv.Atoi(os.Args[1])
	if err != nil {
		fmt.Println("Invalid index")
		os.Exit(1)
	}
	fmt.Printf("Starting client %d\n", index)

	serverName := utils.GetServerName(index)
	serverPort := utils.GetServerPort(index)

	addr := serverName + serverPort
	fmt.Printf("[CLIENT %d] Connecting to server %s\n", index, addr)

	conn, err := rpc.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Failed to connect to server", index)
		os.Exit(1)
	}
	defer conn.Close()

	connRequest := utils.NewNumberedArg("", os.Args[1], 0)
	err = conn.Call("sequential.EstablishFirstConnection", connRequest, utils.NewResponse())
	if err != nil {
		fmt.Println("Failed to connect to server", index, "with error: ", err)
	}

	args := utils.NewArg("testKey", "testValue")
	resp := utils.NewResponse()
	err = conn.Call("sequential.Put", args, resp)

	if err != nil {
		fmt.Println("Error in call to Put")
	}

	err = conn.Call("sequential.Get", args, resp)

	if err == nil && resp.IsPrintable {
		fmt.Printf("Answer from server: Value = %s\n", resp.Value)
	}

	//QUI DEVE GENERARSI L'ERRORE INVECE, perché la connessione è già stata stabilita.
	err = conn.Call("sequential.EstablishFirstConnection", connRequest, utils.NewResponse())
	if err != nil {
		fmt.Println("Failed to connect to server", index, "with error: ", err)
	}

	fmt.Println("Done")

}
