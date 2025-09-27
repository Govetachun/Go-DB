package utils

import "math/rand"

func RandomInt() int {
	return rand.Intn(10000)
}

// Assert panics with message if condition is false
func Assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}


