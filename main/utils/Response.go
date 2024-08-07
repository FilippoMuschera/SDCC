package utils

type Response struct {
	Value       string
	IsPrintable bool
}

func NewResponse() *Response {
	return &Response{Value: "", IsPrintable: false} //inizializzazione
}
