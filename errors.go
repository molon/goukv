package goukv

import "errors"

// error related variables
var (
	ErrDriverAlreadyExists = errors.New("the specified driver name is already exists")
	ErrDriverNotFound      = errors.New("the requested driver isn't found")
	ErrNoScanner           = errors.New("the scanner is required")
	ErrScanDone            = errors.New("this scan has ended")
	ErrKeyNotFound         = errors.New("the specified key couldn't be found")
)
