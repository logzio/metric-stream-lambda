package handler

import (
	"errors"
	"fmt"
)

type errorCollector []error

func (c *errorCollector) Collect(e error) { *c = append(*c, e) }

func (c *errorCollector) Length() int {
	return len(*c)
}
func (c *errorCollector) Error() error {
	err := "Collected errors:\n"
	for i, e := range *c {
		err += fmt.Sprintf("\tError %d: %s\n", i, e.Error())
	}
	return errors.New(err)
}
