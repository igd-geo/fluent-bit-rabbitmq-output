package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

func CreateRoutingKey(rk string, record *map[string]interface{}, rkDelimiter string) (string, error) {
	var recordAccessorLookupTable map[string]string
	var builder strings.Builder

	recordAccessorRegex, err := regexp.Compile(`(\'[^\s\']+\')|(\"[^\s\"]+\")|(\[0\])|(\[[1-9][0-9]*\])`)
	if err != nil {
		return "", err
	}

	recordAccessorLookupTable = make(map[string]string)

	recordAccessors := strings.Split(rk, rkDelimiter)

	for idx, recordAccessor := range recordAccessors {
		if strings.HasPrefix(recordAccessor, "$") {

			val, containsRecordAccessor := recordAccessorLookupTable[recordAccessor]

			if containsRecordAccessor {
				builder.WriteString(val)
			} else {
				subRk, err := extractValueFromRecord(record, recordAccessorRegex.FindAllString(recordAccessor, -1))
				if err != nil {
					return "", err
				}

				recordAccessorLookupTable[recordAccessor] = subRk
				builder.WriteString(subRk)
			}
		} else {
			builder.WriteString(recordAccessor)
		}

		if idx != (len(recordAccessors) - 1) {
			builder.WriteString(rkDelimiter)
		}
	}

	return builder.String(), nil
}

func extractValueFromRecord(record *map[string]interface{}, keys []string) (string, error) {

	if len(keys) > 0 {
		arrKey := []rune(keys[0])
		currentKey := string(arrKey[1:(len(arrKey) - 1)])

		if strings.HasPrefix(keys[0], "[") {
			return "", fmt.Errorf("Couldn't access the Record with the array-accessor '%s', record-accessor is required", currentKey)
		}

		val, recordContainsKey := (*record)[currentKey]
		if len(keys) == 1 {
			if recordContainsKey {
				if removeRkValuesFromRecord {
					delete(*record, currentKey)
				}
				return fmt.Sprintf("%v", val), nil
			}
			return "", fmt.Errorf("Can't access the record with the given record-accessor '%s'", currentKey)
		}

		subRecord, recordContainsSubRecord := val.(map[string]interface{})
		if recordContainsSubRecord {
			return extractValueFromRecord(&subRecord, keys[1:])
		}

		recordArray, recordContainsArray := val.(*[]interface{})
		if recordContainsArray {
			return extractValueFromArray(recordArray, keys[1:])
		}

		return "", fmt.Errorf("Couldn't access the Record with the record-accessor '%s'", currentKey)
	}

	return "", fmt.Errorf("The given routing-key doesn't contain any values")
}

func extractValueFromArray(recordArray *[]interface{}, keys []string) (string, error) {

	if len(keys) > 0 {
		arrKey := []rune(keys[0])
		currentKey := string(arrKey[1:(len(arrKey) - 1)])
		idx, err := strconv.Atoi(currentKey)
		arr := *recordArray

		if err != nil {
			return "", fmt.Errorf("Couldn't parse the array-accessor '%s' to int", currentKey)
		}

		if strings.HasPrefix(keys[0], "\"") || strings.HasPrefix(keys[0], "'") {
			return "", fmt.Errorf("Couldn't access the array with the record-accessor '%s', array-accessor is required", currentKey)
		}

		if len(arr) <= idx {
			return "", fmt.Errorf("The given index '%s' exceededs the array-size", currentKey)
		}

		val := arr[idx]

		if len(keys) == 1 {
			if removeRkValuesFromRecord {
				copy(arr[idx:], arr[idx+1:])
				arr[len(arr)-1] = ""
				arr = arr[:len(arr)-1]
				*recordArray = arr
			}
			return fmt.Sprintf("%v", val), nil
		}

		subRecord, recordContainsSubRecord := val.(map[string]interface{})
		if recordContainsSubRecord {
			return extractValueFromRecord(&subRecord, keys[1:])
		}

		recordArray, recordContainsArray := val.(*[]interface{})
		if recordContainsArray {
			return extractValueFromArray(recordArray, keys[1:])
		}

		return "", fmt.Errorf("Couldn't access the Record with the record-accessor '%s'", currentKey)
	}

	return "", fmt.Errorf("The given routing-key doesn't contain any values")
}
