package goukv

import "errors"

// error related variables
var (
	ErrDriverAlreadyExists = errors.New("the specified driver name is already exisrs")
	ErrDriverNotFound      = errors.New("the requested driver isn't found")
	ErrKeyExpired          = errors.New("the specified key is expired")
	ErrNoScanner           = errors.New("the Scanner is required")
	ErrScanDone            = errors.New("this scan has ended")
)
