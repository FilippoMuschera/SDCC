package utils

type Args struct {
	Key           string
	Value         string
	RequestNumber int
	ClientIndex   int
}

func NewArg(key string, value string, requestNumber int, clientIndex int) *Args {
	args := &Args{Key: key, Value: value, RequestNumber: requestNumber, ClientIndex: clientIndex}
	return args
}
