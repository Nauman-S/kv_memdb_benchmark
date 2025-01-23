package util

import "crypto/rand"

func GenerateRandomData() ([]byte, []byte) {
	key := make([]byte, 32)
	val := make([]byte, 32)
	_, err := rand.Read(key)
	if err != nil {
		panic(err) // handle error as needed
	}
	_, err = rand.Read(val)
	if err != nil {
		panic(err) // handle error as needed
	}
	return key, val
}
