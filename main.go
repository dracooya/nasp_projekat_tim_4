package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

//Objekat koji ima sva podesavanja za projekat
type ConfigObj struct {

	//WAL
	batch_size   int
	segment_size int
	low_w_mark   int

	//Token Bucket
	tokens  int
	minutes float64

	//LRU Cache
	cache_limit int

	//Memtable
	mem_max_size int
	threshold    float64 //procenentualno broj nakon kojeg se Flush-uje

	//Bloom filter
	bloom_precision float64

	//LSM stabla i kompakcije
	max_height      int //max visina lsm stabla (BEZ Memtabele)
	compaction_size int //broj tabela koje se spajaju
}

//Kreira objekat sa podrazumevanim vrednostima
func Default() *ConfigObj {
	return &ConfigObj{
		batch_size:   3,
		segment_size: 6,
		low_w_mark:   3,

		tokens:  100,
		minutes: 1,

		cache_limit: 3,

		mem_max_size: 5,
		threshold:    80,

		bloom_precision: 0.1,

		max_height:      3,
		compaction_size: 2,
	}
}

//Funkcija proverava ispravnost vrednosti u eksternoj konfiguraciji za CELE BROJEVE
//min i max su opsezi u kojima se vrednost moze naci
//Vraca indikator - true = ispravno, false = neispravno i konvertovanu vrednost ukoliko je tacno, -1 ukoliko je netacno
func CheckValInt(val string, min int, max int) (bool, int) {
	value, err := strconv.Atoi(val)
	if err != nil { //podatak nije celobrojnog tipa
		return false, -1
	} else {
		if value < min || value > max { //van opsega
			return false, -1
		} else {
			return true, value //sve je uredu
		}
	}

}

//Funkcija proverava ispravnost vrednosti u eksternoj konfiguraciji za REALNE BROJEVE
//min i max su opsezi u kojima se vrednost moze naci
//Vraca indikator - true = ispravno, false = neispravno i konvertovanu vrednost ukoliko je tacno, -1 ukoliko je netacno
func CheckValFloat(val string, min float64, max float64) (bool, float64) {
	value, err := strconv.ParseFloat(val, 64)
	if err != nil { //podatak nije realnog tipa
		return false, -1
	} else {
		if value < min || value > max { //van opsega
			return false, -1
		} else {
			return true, value //sve je uredu
		}
	}

}

//Funkcija iscitava eksterni konfiguracioni fajl na osnovu prosledjene putanje i proverava valjanost vrednosti
//Menja konfiguracioni objekat (atribute) ukoliko je ispravna vrednost
//TODO:Prilagodi opsege vrednosti za svaki atribut!
func (config *ConfigObj) ReadConfig(path string) {
	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		pair := strings.Split(line, "=")
		switch pair[0] {
		case "batchSize":
			correct, val := CheckValInt(pair[1], 0, 5)
			if correct {
				config.batch_size = val
			} else {
				println("batch size neispravan. Koristi se default.")
			}

		case "segmentSize":
			correct, val := CheckValInt(pair[1], 0, 10)
			if correct {
				config.segment_size = val
			} else {
				println("segment size neispravan. Koristi se default.")
			}

		case "lowWaterMark":
			correct, val := CheckValInt(pair[1], 0, 5)
			if correct {
				config.low_w_mark = val
			} else {
				println("low water mark neispravan. Koristi se default.")
			}

		case "tokens":
			correct, val := CheckValInt(pair[1], 1, 10000)
			if correct {
				config.tokens = val
			} else {
				println("tokens neispravan. Koristi se default.")
			}

		case "minutes":
			correct, val := CheckValFloat(pair[1], 0.1, 10000)
			if correct {
				config.minutes = val
			} else {
				println("minutes neispravan. Koristi se default.")
			}

		case "memMaxSize":
			correct, val := CheckValInt(pair[1], 1, 100000)
			if correct {
				config.mem_max_size = val
			} else {
				println("mem max size size neispravan. Koristi se default.")
			}

		case "memThreshold":
			correct, val := CheckValFloat(pair[1], 0.1, 100)
			if correct {
				config.threshold = val
			} else {
				println("memtable threshold neispravan. Koristi se default.")
			}

		case "bloomPrecision":
			correct, val := CheckValFloat(pair[1], 0.1, 1)
			if correct {
				config.bloom_precision = val
			} else {
				println("bloom precision neispravan. Koristi se default.")
			}
		case "maxHeightLSM":
			correct, val := CheckValInt(pair[1], 1, 15)
			if correct {
				config.max_height = val
			} else {
				println("LSM max height neispravan. Koristi se default.")
			}

		case "compactionSize":
			correct, val := CheckValInt(pair[1], 0, 10)
			if correct {
				config.compaction_size = val
			} else {
				println("compaction size neispravan. Koristi se default.")
			}
		default:
			println("Parametar ne postoji!")
		}

	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func (config *ConfigObj) PrintConfig() {
	println("Batch size:" + strconv.Itoa(config.batch_size))
	println("Segment size:" + strconv.Itoa(config.segment_size))
	println("Low water mark:" + strconv.Itoa(config.low_w_mark))
	println("Tokens:" + strconv.Itoa(config.tokens))
	println("Minutes:", config.minutes)
	println("Memtable max size:" + strconv.Itoa(config.mem_max_size))
	println("Memtable threshold:", config.threshold)
	println("Bloom filter precision:", config.bloom_precision)
	println("LSM tree max height:" + strconv.Itoa(config.max_height))
	println("Compaction size:" + strconv.Itoa(config.compaction_size))

}

func main() {
	/*config := Default()
	config.ReadConfig("config.txt")
	config.PrintConfig()*/
	idk := []byte{0}
	line := string(idk)
	fmt.Println(line)

	err := InitWAL()
	if err != nil {
		fmt.Println(err)
		return
	}

	err = log.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
}
