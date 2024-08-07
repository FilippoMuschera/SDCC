package main

import (
	"SDCC/main/utils"
	"errors"
	"fmt"
	"sync"
)

// KVSSequential is a concrete implementation of the KVS interface
type KVSSequential struct {
	store map[string]string
	mu    sync.Mutex //mutex per accedere alla Map
}

// NewKVSSequential creates a new instance of KVSSequential
func NewKVSSequential() *KVSSequential {
	return &KVSSequential{
		store: make(map[string]string),
	}
}

// Get retrieves a value from the key-value store
func (kvs *KVSSequential) Get(args utils.Args, reply *utils.Response) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	fmt.Println("Serving GET operation")

	value, ok := kvs.store[args.Key]
	if !ok {
		return errors.New("key not found")
	}
	fmt.Println("value in GET = ", value)

	reply.Value = value
	reply.IsPrintable = true
	return nil
}

// Put stores a value in the key-value store
func (kvs *KVSSequential) Put(args utils.Args, reply *utils.Response) error {
	kvs.mu.Lock()
	fmt.Println("Serving PUT operation")
	defer kvs.mu.Unlock()

	kvs.store[args.Key] = args.Value
	reply.Value = ""
	return nil
}

// Delete removes a key from the key-value store
func (kvs *KVSSequential) Delete(args utils.Args, reply *utils.Response) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	_, ok := kvs.store[args.Key]
	if !ok {
		return errors.New("key not found")
	}

	delete(kvs.store, args.Key)
	reply.Value = ""
	return nil
}
