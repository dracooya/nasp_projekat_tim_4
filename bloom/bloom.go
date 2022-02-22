package bloom

import (
	"bufio"
	"github.com/spaolacci/murmur3"
	"hash"
	"log"
	"math"
	"os"
	"strconv"
	"time"
)

//Put funkcija
//prosledjuje se niz kljuceva od kojih se formira bloom filter, ocekivani broj elemenata(max) i "tacnost"
func NewBloom(keys []string, falsePositiveRate float64) (string, []uint32) {
	expectedElements := len(keys)
	if expectedElements < 100 {
		expectedElements = 100
	}
	m := CalculateM(expectedElements, falsePositiveRate)
	k := CalculateK(expectedElements, m)
	bloom := ""
	for i := 0; i < int(m); i++ {
		bloom = bloom + "0"
	}
	hashes, seeds := CreateHashFunctions(k)

	for _, key := range keys {
		bloom = AddKey(bloom, key, hashes)
	}
	return bloom, seeds
}

func WriteBloom(bloom string, seeds []uint32, name string) {
	file, err := os.OpenFile("data/SSTable"+name+"/filter"+name+".txt", os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	file.Write([]byte(bloom))
	file.WriteString("\n")
	//Write seeds
	for _, seed := range seeds {
		file.WriteString(strconv.Itoa(int(seed)) + " ")
	}
}

func LoadBool(name string) (string, []uint32) {
	file, err := os.Open("data/SSTable" + name + "/filter" + name + ".txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	//First scan for bool filter
	success := scanner.Scan()
	if success == false {
		err = scanner.Err()
		if err == nil {
		} else {
			log.Fatal(err)
		}
	}
	bloom := scanner.Text()
	//x scans for hash seeds
	seeds := make([]uint32, 0)
	for {
		success := scanner.Scan()
		if success == false {
			err = scanner.Err()
			if err == nil {
				break
			} else {
				log.Fatal(err)
			}
		}
		temp, _ := strconv.Atoi(scanner.Text())
		seeds = append(seeds, uint32(temp))
	}
	return bloom, seeds
}

//Funkcija se koristi u NewBloom, ali ako je potrebno uneti samo jedan kljuc moze biti korisna
func AddKey(bloom string, key string, hashes []hash.Hash32) string {
	for _, ha := range hashes {
		hashed := ha.Sum([]byte(key))
		for _, h := range hashed {
			bloom = bloom[:int(h)] + "1" + bloom[int(h)+1:]
		}
	}
	return bloom
}

//Get funkcija proverava postojanje kljuca u bloom filteru
//potrebno proslediti bloom filter, kljuc koji se trazi i hash funkcije generisane "CreateHashFunctions"metodom
func IsInBloom(bloom string, key string, seeds []uint32) bool {
	hashes := CreateHashFunctionsFromSeeds(seeds)
	for _, ha := range hashes {
		hashed := ha.Sum([]byte(key))
		for _, h := range hashed {
			if bloom[int(h)] != '1' {
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

func CreateHashFunctions(k uint) ([]hash.Hash32, []uint32) {
	h := make([]hash.Hash32, 0)
	ts := uint(time.Now().Unix())
	seed := make([]uint32, 0)
	for i := uint(0); i < k; i++ {
		temp := uint32(ts + i)
		seed = append(seed, temp)
		h = append(h, murmur3.New32WithSeed(temp))
	}
	return h, seed
}

func CreateHashFunctionsFromSeeds(seeds []uint32) []hash.Hash32 {
	h := make([]hash.Hash32, 0)
	for _, seed := range seeds {
		h = append(h, murmur3.New32WithSeed(seed))
	}
	return h
}
