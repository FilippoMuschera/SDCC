package utils

type Args struct {
	Key           string
	Value         string
	RequestNumber int
	ClientIndex   int
	OpType        string
}

func NewArg(key string, value string, requestNumber int, clientIndex int, opType string) *Args {
	args := &Args{Key: key, Value: value, RequestNumber: requestNumber, ClientIndex: clientIndex, OpType: opType}
	return args
}
