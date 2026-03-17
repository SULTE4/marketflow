package domain

import "errors"

var (
	ErrTickerNotFound = errors.New("ticker not found")
	ErrInvalidInput   = errors.New("invalid input")
	ErrInternalError  = errors.New("internal error")
	ErrNoData         = errors.New("no data found")
)
