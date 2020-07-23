package sstable

import "errors"

func NotFoundError(key string) error {
	return errors.New(key + " not found ")
}

var NoNextElementError = errors.New("No Next Element ")

func IllegalArgumentErrors(argument, val string) error {
	return errors.New("argument [" + argument + "] with wrong value: " + val)
}

var OutOfIndexError = errors.New("Out Of Index ")
