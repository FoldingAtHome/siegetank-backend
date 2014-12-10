package scv

import (
	"math/rand"
)

func RandSeq(n int) string {
	b := make([]rune, n)
	var letters = []rune("012345689ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
