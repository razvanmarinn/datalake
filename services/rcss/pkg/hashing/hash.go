package hashing

import "hash/fnv"

func FNV32a(text string, batches int) uint32 {
	algorithm := fnv.New32a()
	algorithm.Write([]byte(text))
	return algorithm.Sum32()
}
