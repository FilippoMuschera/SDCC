package main

import "SDCC/main/utils"

type (
	KVS interface {
		Get(args utils.Args, reply *utils.Response) error
		Put(args utils.Args, reply *utils.Response) error
		Delete(args utils.Args, reply *utils.Response) error
		EstablishFirstConnection(args utils.Args, reply *utils.Response) error
	}
)
