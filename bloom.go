package main

import (
	"github.com/spaolacci/murmur3"
	"hash"
	"math"
	"strconv"
	"time"
)

//Put funkcija
//prosledjuje se niz kljuceva od kojih se formira bloom filter, ocekivani broj elemenata(max) i "tacnost"
func NewBloom(keys []int, expectedElements int, falsePositiveRate float64) ([]int, []hash.Hash32) {
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	bloom := make([]int, 0)
	for i := 0; i < int(m); i++ {
		bloom = append(bloom, 0)
	}
	hashes := CreateHashFunctions(k)

	for _, key := range keys {
		AddKey(bloom, key, hashes)
	}
	return bloom, hashes
}

//Funkcija se koristi u NewBloom, ali ako je potrebno uneti samo jedan kljuc moze biti korisna
func AddKey(bloom []int, key int, hashes []hash.Hash32) {
	for _, oneHash := range hashes {
		hashed := oneHash.Sum([]byte(strconv.Itoa(key)))
		for _, h := range hashed {
			bloom[int(h)] = 1
		}
	}
}

//Get funkcija proverava postojanje kljuca u bloom filteru
//potrebno proslediti bloom filter, kljuc koji se trazi i hash funkcije generisane "CreateHashFunctions"metodom
func IsInBloom(bloom []int, key int, hashes []hash.Hash32) bool {
	for _, oneHash := range hashes {
		hashed := oneHash.Sum([]byte(strconv.Itoa(key)))
		for _, h := range hashed {
			if bloom[int(h)] != 1 {
				return false
			}
		}
	}
	return true
}

//Pomocne metode
func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func CreateHashFunctions(k uint) []hash.Hash32 {
	h := make([]hash.Hash32, 0)
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		h = append(h, murmur3.New32WithSeed(uint32(ts+1)))
	}
	return h
}
