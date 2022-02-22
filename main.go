package main

import (
	"bufio"
	"fmt"
	"main/SSTable"
	"main/kompakcije"
	"os"
	"strconv"
	"strings"
)

//Strukture koje se nalaze u memoriji i konfiguracioni objekat sa iscitanom konfiguracijom
type System struct {
	memtable *Memtable
	cache    *Cache
	config   *ConfigObj
}

var (
	system *System
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

func (sys *System) put(user string, key string, value []byte) bool {
	if user != "" {
		if !CheckTokenBucket(user) { // korisnik nema vise tokena
			return false
		}
	}
	err, wal_in := log.WritePutBuffer(key, value)
	if err != nil {
		fmt.Println(err)
		return false
	}

	var full_percent float64
	full_percent = 0
	if sys.memtable.curr_size != 0 {
		full_percent = (float64(sys.memtable.curr_size) / float64(sys.memtable.max_size)) * 100
	}

	if full_percent >= sys.memtable.threshold {
		println("puno")
		data, err := sys.memtable.Flush()
		if err == nil {
			SSTable.MakeTable(data, 1, sys.config.bloom_precision)
		} else {
			fmt.Println(err)
			return false
		}
	}

	_, err = sys.memtable.PutElement(wal_in)
	if err != nil {
		fmt.Println(err)
		return false
	}

	return true

}

func (sys *System) get(user string, key string) []byte {
	if user != "" {
		if !CheckTokenBucket(user) { // korisnik nema vise tokena
			return nil
		}
	}
	//Ako nije pronadjeno u kesu i mem tabili
	mem_val := sys.memtable.GetElement(key)
	if mem_val == nil {
		cache_val := sys.cache.Search(key) //ukoliko je podatak u cache-u,on ga automatski propagira na prvo mesto
		if cache_val == nil {
			value, found := SSTable.Find(key, sys.config.max_height)
			if found {
				//print("iz ss-a je.")
				sys.cache.Insert(key, value)
				return value
			} else {
				return nil
			}
		} else {
			//print("iz kesa je.")
			return cache_val.Value.(KV).value
		}
	} else {
		//print("iz mema je.")
		sys.cache.Insert(key, mem_val)
		return mem_val
	}
	return nil

}

func (sys *System) Delete(user string, key string) bool {
	if user != "" {
		if !CheckTokenBucket(user) { // korisnik nema vise tokena
			return false
		}
	}
	err := log.WriteDeleteBuffer(key)
	if err != nil {
		fmt.Println(err)
		return false
	}
	sys.cache.DeleteKey(key)
	if sys.memtable.DeleteElement(key) == false {
		if SSTable.Delete(key, sys.config.max_height) {
			return true
		} else {
			return false
		}
	}

	return true
}

//Incijalizacija memtabele, cache-a i konfiguracionog objekta
func CreateSystem() *System {
	config_obj := Default()
	config_obj.ReadConfig("config.txt")
	return &System{
		memtable: NewMemtable(config_obj.mem_max_size, config_obj.threshold),
		cache:    createCache(config_obj.cache_limit),
		config:   config_obj,
	}
}

//Kreira objekat sa podrazumevanim vrednostima
func Default() *ConfigObj {
	return &ConfigObj{
		batch_size:   3,
		segment_size: 6,
		low_w_mark:   3,

		tokens:  50,
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
			correct, val := CheckValInt(pair[1], 1, 15)
			if correct {
				config.batch_size = val
			} else {
				println("batch size neispravan. Koristi se default.")
			}

		case "segmentSize":
			correct, val := CheckValInt(pair[1], 2, 10)
			if correct {
				config.segment_size = val
			} else {
				println("segment size neispravan. Koristi se default.")
			}

		case "lowWaterMark":
			correct, val := CheckValInt(pair[1], 1, 10)
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
			correct, val := CheckValFloat(pair[1], 1, 10)
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
			correct, val := CheckValFloat(pair[1], 0.000001, 0.9)
			if correct {
				config.bloom_precision = val
			} else {
				println("bloom precision neispravan. Koristi se default.")
			}
		case "maxHeightLSM":
			correct, val := CheckValInt(pair[1], 1, 10)
			if correct {
				config.max_height = val
			} else {
				println("LSM max height neispravan. Koristi se default.")
			}

		case "compactionSize":
			correct, val := CheckValInt(pair[1], 2, 10)
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
	err := InitWAL()
	if err != nil {
		fmt.Println(err)
		return
	}

	system = CreateSystem()
	InitializeTokenBucketConfigs(uint32(system.config.tokens), uint32(system.config.minutes))
	InitializeWALConfigs(system.config.batch_size, system.config.segment_size, system.config.low_w_mark)

	println(system.put("test", "2", []byte("izmena")))
	println(system.put("test", "1", []byte("prvi testt")))
	println(system.put("test", "3", []byte("treci testt")))
	println(system.put("test", "4", []byte("cetvrti testt")))

	println(system.put("test", "22", []byte("izmena")))
	println(system.put("test", "11", []byte("prvi testt")))
	println(system.put("test", "33", []byte("treci testt")))
	println(system.put("test", "44", []byte("cetvrti testt")))

	println(system.put("test", "222", []byte("izmena")))
	println(system.put("test", "111", []byte("prvi testt")))
	println(system.put("test", "333", []byte("treci testt")))
	println(system.put("test", "444", []byte("cetvrti testt")))
	//println(system.Delete("test", "2"))
	//println(string(system.get("test", "2")))
	kompakcije.Kompakcija(system.config.compaction_size, system.config.max_height, system.config.bloom_precision)
	//println(string(system.get("test", "2")))
	//println(string(system.get("test", "2")))
	//println(system.put("test", "", []byte("cetvrti test")))
	/*println(string(system.get("test", "2")))
	println(system.put("test", "67", []byte("prvi testt")))
	println(string(system.get("test", "67")))
	println(system.put("test", "67", []byte("izmena")))
	println(string(system.get("test", "67")))*/
	/*system.cache.printCache()
	println(string(system.get("test", "2")))
	/*println(system.put("test", "5", []byte("peti test")))
	println(string(system.get("test", "5")))
	println(system.Delete("test", "5"))
	println(string(system.get("test", "5")))
	/*system.Delete("test", "2")
	system.put("test", "2", []byte("drugi test"))
	system.put("test", "1", []byte("prvi test"))
	system.put("test", "2", []byte("drugi test"))
	system.put("test", "1", []byte("prvi test"))
	system.put("test", "2", []byte("drugi test"))
	system.put("test", "1", []byte("prvi test"))
	system.put("test", "2", []byte("drugi test"))
	system.put("test", "1", []byte("prvi test"))
	system.put("test", "2", []byte("drugi test"))
	fmt.Println(system.get("test", "2"))
	system.Delete("test", "2")
	system.put("test3", "2", []byte("drugi test"))
	fmt.Println(system.get("test3", "2"))
	system.Delete("test3", "2")*/

	err = log.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
}
