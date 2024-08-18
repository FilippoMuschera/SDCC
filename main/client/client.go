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

	args := utils.NewArg("testKey", "testValue", 1, index)
	resp := utils.NewResponse()
	err = conn.Call("sequential.Put", args, resp)

	if err != nil {
		fmt.Println("Error in call to Put:", err)
	}

	args.RequestNumber = 2
	err = conn.Call("sequential.Get", args, resp)

	if err == nil && resp.IsPrintable {
		fmt.Printf("Answer from server: Value = %s\n", resp.Value)
	}

	args.RequestNumber = 3
	args.Value = "AnotherValue"
	err = conn.Call("sequential.Put", args, resp)

	if err != nil {
		fmt.Println("Error in call to Put:", err)
	}

	args.RequestNumber = 4
	err = conn.Call("sequential.Get", args, resp)

	if err == nil && resp.IsPrintable {
		fmt.Printf("Answer from server: Value = %s\n", resp.Value)
	}

	args.RequestNumber = 5
	err = conn.Call("sequential.Delete", args, resp)

	if err != nil {
		fmt.Println("Error in call to Put:", err)
	}

	args.RequestNumber = 6
	err = conn.Call("sequential.Get", args, resp)

	if err == nil && resp.IsPrintable {
		fmt.Printf("Answer from server: Value = %s\n", resp.Value)
	} else if err != nil {
		fmt.Println("Error in call to Get (as I should here):", err)
	}

	fmt.Println("Done")

}
