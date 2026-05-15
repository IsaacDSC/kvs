package dto

import (
	"encoding/json"
	"errors"
)

type FieldError struct {
	errs []error
	err  error
}

func NewFieldError() *FieldError {
	return &FieldError{}
}

func (f *FieldError) AddErr(err error) {
	if err == nil {
		return
	}
	f.errs = append(f.errs, err)
}

func (f *FieldError) Build() *FieldError {
	if len(f.errs) == 0 {
		return nil
	}
	f.err = errors.Join(f.errs...)
	return f
}

func (f FieldError) Json() []byte {
	data := struct {
		Error   string   `json:"error"`
		Details []string `json:"details"`
	}{}

	if f.err != nil {
		data.Error = f.err.Error()
	}
	for _, e := range f.errs {
		data.Details = append(data.Details, e.Error())
	}

	b, err := json.Marshal(data)
	if err != nil {
		return []byte("error enconde when response error on validate")
	}

	return b
}
