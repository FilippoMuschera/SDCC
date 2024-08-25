package utils

type Response struct {
	Key         string
	Value       string
	IsPrintable bool
}

func NewResponse() *Response {
	return &Response{Value: "", IsPrintable: false, Key: ""} //inizializzazione
}
