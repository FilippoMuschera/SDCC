package utils

type Args struct {
	Key       string
	Value     string
	msgNumber int
}

func NewArg(key string, value string) *Args {
	args := &Args{Key: key, Value: value, msgNumber: 0}
	return args
}

func NewNumberedArg(key string, value string, num int) *Args {
	args := &Args{Key: key, Value: value, msgNumber: num}
	return args
}
