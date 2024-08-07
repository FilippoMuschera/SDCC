package utils

type Args struct {
	Key   string
	Value string
}

func NewArg(key string, value string) *Args {
	args := &Args{Key: key, Value: value}
	return args
}
