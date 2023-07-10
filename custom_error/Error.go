package custom_error

import (
	"log"
)

type Error struct {
	msg string
	err error
}

func New(msg string, error error) *Error {
	return &Error{msg, error}
}

func (e *Error) Error() string {
	return e.msg + ": " + e.getCause()
}

func (e *Error) Log() *Error {
	log.Println(e)
	return e
}

func (e *Error) getCause() string {
	if e.err != nil {
		return e.err.Error()
	}

	return ""
}
