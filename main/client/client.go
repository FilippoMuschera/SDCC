package main

import (
	"SDCC/main/utils"
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

func init() {
	if os.Getenv("DEBUG") == "1" {
		fmt.Println("Waiting for debugger to attach...")
		time.Sleep(15 * time.Second) // Pause for 10 seconds to attach debugger
	}
}

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

	// Channel to receive RPC responses
	doneChan := make(chan *rpc.Call, 7)

	// Asynchronous Put call
	args := utils.NewArg("testKey", "testValue", 1, index)
	resp := utils.NewResponse()
	conn.Go("sequential.Put", args, resp, doneChan)

	// Asynchronous Get call
	args.RequestNumber = 2
	resp2 := utils.NewResponse()
	conn.Go("sequential.Get", args, resp2, doneChan)

	// Asynchronous Put call with another value
	args.RequestNumber = 3
	args.Value = "AnotherValue"
	resp3 := utils.NewResponse()
	conn.Go("sequential.Put", args, resp3, doneChan)

	// Asynchronous Get call
	args.RequestNumber = 4
	resp4 := utils.NewResponse()
	conn.Go("sequential.Get", args, resp4, doneChan)

	// Asynchronous Delete call
	args.RequestNumber = 5
	resp5 := utils.NewResponse()
	conn.Go("sequential.Delete", args, resp5, doneChan)

	// Asynchronous Get call (expected to fail)
	args.RequestNumber = 6
	resp6 := utils.NewResponse()
	conn.Go("sequential.Get", args, resp6, doneChan)

	// Asynchronous Put call
	args.RequestNumber = 7
	args.Key = "DeveRimanere"
	args.Value = "FinoAllaFine"
	resp7 := utils.NewResponse()
	conn.Go("sequential.Put", args, resp7, doneChan)

	// Asynchronous Put call to signal end
	args2 := utils.NewArg(utils.EndKey, utils.EndValue, 8, index)
	resp8 := utils.NewResponse()
	conn.Go("sequential.Put", args2, resp8, doneChan)

	// Wait for the RPC calls to complete
	for i := 0; i < 7; i++ {
		call := <-doneChan
		switch i {
		case 1:
			if call.Error == nil && resp2.IsPrintable {
				fmt.Printf("Answer from server: Value = %s\n", resp2.Value)
			}
		case 3:
			if call.Error == nil && resp4.IsPrintable {
				fmt.Printf("Answer from server: Value = %s\n", resp4.Value)
			}
		case 5:
			if call.Error == nil && resp6.IsPrintable {
				fmt.Printf("Answer from server: Value = %s\n", resp6.Value)
			} else if call.Error != nil {
				fmt.Println("Error in call to Get (as I should here):", call.Error)
			}
		}
	}

	fmt.Println("Done")
}
