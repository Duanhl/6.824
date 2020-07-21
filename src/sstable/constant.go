package sstable

type NotFoundKeyError struct {
	errMsg string
}

func (nfe *NotFoundKeyError) Error() string {
	return nfe.errMsg
}

func NotFoundError(key string) *NotFoundKeyError {
	return &NotFoundKeyError{errMsg: key + " not found"}
}

type NoNextError struct{}

func (nne *NoNextError) Error() string {
	return "No Next Element"
}

func NoNextElementError() *NoNextError {
	return &NoNextError{}
}
