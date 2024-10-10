package internal

import (
	"errors"
	"fmt"
)

type ErrorCollector []error

func (c *ErrorCollector) Collect(e error) { *c = append(*c, e) }

func (c *ErrorCollector) Length() int {
	return len(*c)
}
func (c *ErrorCollector) Error() error {
	err := "Collected errors:\n"
	for i, e := range *c {
		err += fmt.Sprintf("\tError %d: %s\n", i, e.Error())
	}
	return errors.New(err)
}
