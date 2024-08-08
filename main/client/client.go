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

	connRequest := utils.NewArg("", "", 1, index)
	err = conn.Call("sequential.EstablishFirstConnection", connRequest, utils.NewResponse())
	if err != nil {
		fmt.Println("Failed to connect to server", index, "with error: ", err)
	}

	args := utils.NewArg("testKey", "testValue", 2, index)
	resp := utils.NewResponse()
	err = conn.Call("sequential.Put", args, resp)

	if err != nil {
		fmt.Println("Error in call to Put")
	}

	args.RequestNumber = 3
	err = conn.Call("sequential.Get", args, resp)

	if err == nil && resp.IsPrintable {
		fmt.Printf("Answer from server: Value = %s\n", resp.Value)
	}

	/* TEST PER CONTROLLARE CHE L'ORDINAMENTO FIFO DELLE RICHIESTE NEL SERVER SIA EFFETTIVAMENTE RISPETTATO: DEBUG
	go func() {
		println("Entering goroutine")
		time.Sleep(5 * time.Second)
		println("done sleeping")
		args := utils.NewArg("testKey", "testValue", 2, index)
		resp := utils.NewResponse()
		err = conn.Call("sequential.Put", args, resp)

		if err != nil {
			fmt.Println("Error in call to Put")
		}
		println("exiting goroutine")
	}()

	args2 := utils.NewArg("testKey", "", 3, index)
	response2 := utils.NewResponse()
	fmt.Println("Calling GET operation")
	err = conn.Call("sequential.Get", args2, response2)

	if err == nil && response2.IsPrintable {
		fmt.Printf("Answer from server: Value = %s\n", response2.Value)
	}*/

	fmt.Println("Done")

}
