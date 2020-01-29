package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// RoutingKeyIsValid checks if a routing key is valid
func RoutingKeyIsValid(rk string, rkDelimiter string) error {
	r, err := regexp.Compile(`^\$((\[\"([^\s\"]+)\"\])|(\[\'([^\s\']+)\'\])|(\[[1-9][0-9]*\])|(\[[0]\]))+$`)
	if err != nil {
		return err
	}

	if len(rk) <= 0 {
		return errors.New("Routing-Key shouldn't be empty")
	}

	if strings.Contains(rk, rkDelimiter) {
		splittedRk := strings.Split(rk, rkDelimiter)
		if arrayContainsString(splittedRk, "") {
			return errors.New("The given routing-key contains an empty value")
		}
		for _, subRk := range splittedRk {
			if strings.HasPrefix(subRk, "$") {
				if !r.MatchString(subRk) {
					return fmt.Errorf("The record_accessor '%s' is invalid", rk)
				}
			}
		}
	} else {
		if strings.HasPrefix(rk, "$") {
			if !r.MatchString(rk) {
				return fmt.Errorf("The record_accessor '%s' is invalid", rk)
			}
		}
	}
	return nil
}
