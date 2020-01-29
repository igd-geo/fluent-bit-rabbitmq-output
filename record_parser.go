package main

func ParseRecord(mapInterface map[interface{}]interface{}) map[string]interface{} {
	parsedMap := make(map[string]interface{})
	for k, v := range mapInterface {
		switch t := v.(type) {
		case []byte:
			// prevent encoding to base64
			parsedMap[k.(string)] = string(t)
		case map[interface{}]interface{}:
			parsedMap[k.(string)] = ParseRecord(t)
		case []interface{}:
			parsedMap[k.(string)] = parseSubRecordArray(t)
		default:
			parsedMap[k.(string)] = v
		}
	}
	return parsedMap
}

func parseSubRecordArray(arr []interface{}) *[]interface{} {
	for idx, i := range arr {
		switch t := i.(type) {
		case []byte:
			arr[idx] = string(t)
		case map[interface{}]interface{}:
			arr[idx] = ParseRecord(t)
		case []interface{}:
			arr[idx] = parseSubRecordArray(t)
		default:
			arr[idx] = t
		}
	}
	return &arr
}
