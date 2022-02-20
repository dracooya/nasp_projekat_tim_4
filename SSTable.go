package SSTable

import (
	"bloom/bloom"
	"bloom/index"
	"bloom/summary"
	"encoding/binary"
	"errors"
	"log"
	"os"
	"sort"
	"strconv"
)

type Entry struct {
	key      string
	keyLen   uint64
	valueLen uint64
	value    []byte
}

//Main funkcija za upis i kreiranje svih potrebnih fajlova i direktorijuma jedne SSTabele
func MakeTable(memTable [][]byte, level int) {
	//crc 4,timestamp 8,tombstone 1, keySize 8, valueSize 8, key, value
	//Popunjavanje entys
	last := FindLastFile(level)
	name := strconv.Itoa(level) + "_" + strconv.Itoa(last)
	entrys := make([]Entry, 0)

	for _, i := range memTable {
		temp := Entry{}
		temp.value = i
		temp.keyLen = binary.LittleEndian.Uint64(i[13:21])
		temp.valueLen = binary.LittleEndian.Uint64(i[21:29])
		temp.key = string(i[29 : 29+temp.keyLen])
		entrys = append(entrys, temp)
	}

	//for _, i := range entrys {
	//	fmt.Println(i.key)
	//}

	//Sortira entrys alfanumericki po kljucu
	entrys = mySort(entrys)

	//Izdvojimo kljuceve
	keys := make([]string, len(entrys))
	for i := 0; i < len(entrys); i++ {
		keys[i] = entrys[i].key
	}

	//Provera da li postoji direktorijum i potrebni fajlovi
	//Ako ne postoje, kreira ih
	createFiles(name)

	//Kreiranje bloom filtera, a zatim i upis
	filter, seeds := bloom.NewBloom(keys, 0.1)
	bloom.WriteBloom(filter, seeds, name)
	//test za bloom
	//bl, seed := bloom.LoadBool(name)
	//fmt.Println(bloom.IsInBloom(bl, "key5", seed))

	//Upis indexa na disk
	entrysLen := make([]uint64, len(entrys))
	for i, j := range entrys {
		entrysLen[i] = uint64(len(j.value))
	}
	index.NewIndex(entrysLen, name)

	//Upis summaty na disk
	keyLen := make([]uint64, len(entrys))
	for i, j := range entrys {
		keyLen[i] = j.keyLen
	}
	summary.NewSummary(keys, keyLen, name)

	//Make SSTabe file
	writeSSTable(entrys, name)
}

func Find(key string, max int) ([]byte, bool) {
	for i := 1; i < max; i++ {
		for j := FindLastFile(i) - 1; j > 0; j-- {
			name := strconv.Itoa(i) + "_" + strconv.Itoa(j)
			filter, seeds := bloom.LoadBool(name)
			if bloom.IsInBloom(filter, key, seeds) {
				offset, found := summary.Find(key, name)
				if found {
					offset = index.Find(offset, name)
					if found {
						return findInTable(offset, name)
					}
				}
			}
		}
	}
	data := make([]byte, 0)
	return data, false
}

func Delete(key string, max int) bool {
	for i := 1; i < max; i++ {
		for j := FindLastFile(i) - 1; j > 0; j-- {
			name := strconv.Itoa(i) + "_" + strconv.Itoa(j)
			filter, seeds := bloom.LoadBool(name)
			if bloom.IsInBloom(filter, key, seeds) {
				offset, found := summary.Find(key, name)
				if found {
					offset = index.Find(offset, name)
					if found {
						_, found = findInTable(offset, name)
						if found { //Same as Find()
							return deleteAt(offset, name)
						}
					}
				}
			}
		}
	}
	return false
}

func deleteAt(offset uint64, name string) bool {
	file, err := os.Open("data/SSTable" + name + "/SSTable" + name + ".txt")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	file.Seek(int64(offset), 0)
	bytes := make([]byte, 12)
	file.Read(bytes) //Read before keyLen 12B
	bytes = make([]byte, 1)
	file.Read(bytes) //Tombstone
	toombstone := bytes[0]
	if int(toombstone) == 0 { //Same as findInTable()
		//Overwrite toombstone
		file, err = os.OpenFile("data/SSTable"+name+"/SSTable"+name+".txt", os.O_WRONLY, 0666)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		file.Seek(int64(offset+12), 0)
		bytes = make([]byte, 1)
		bytes[0] = byte(1)
		file.Write(bytes)
		return true
	}
	return false
}

func findInTable(offset uint64, name string) ([]byte, bool) {
	value := make([]byte, 0)
	found := false
	file, err := os.Open("data/SSTable" + name + "/SSTable" + name + ".txt")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	file.Seek(int64(offset), 0)
	bytes := make([]byte, 12)
	file.Read(bytes) //Read before keyLen 12B
	bytes = make([]byte, 1)
	file.Read(bytes) //Tombstone
	toombstone := bytes[0]
	if int(toombstone) == 0 {
		bytes = make([]byte, 8)
		file.Read(bytes) //KeyLen8B
		keyLen := binary.LittleEndian.Uint64(bytes)
		file.Read(bytes) //ValueLen8B
		valueLen := binary.LittleEndian.Uint64(bytes)
		bytes = make([]byte, keyLen)
		file.Read(bytes) //Key
		bytes = make([]byte, valueLen)
		file.Read(bytes) //Value
		value = bytes
		found = true
	}
	return value, found
}

func writeSSTable(entrys []Entry, name string) {
	file, err := os.OpenFile("data/SSTable"+name+"/SSTable"+name+".txt", os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for _, i := range entrys {
		_, err = file.Write(i.value)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func mySort(entrys []Entry) []Entry {
	sort.Slice(entrys[:], func(i, j int) bool {
		return entrys[i].key < entrys[j].key
	})
	return entrys
}

//Kreira i brise sve podatke u potrebnim fajlovima
func createFiles(name string) {
	//Direktorijum
	if _, err := os.Stat("data/SSTable" + name); err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir("data/SSTable"+name, os.ModePerm); err != nil {
				log.Fatal(err)
			}
		}
	}
	//Filter
	if _, err := os.Stat("data/SSTable" + name + "/index" + name + ".txt"); err != nil {
		if os.IsNotExist(err) {
			_, err := os.Create("data/SSTable" + name + "/filter" + name + ".txt")
			if err != nil {
				panic(err)
			}
		}
	}
	if err := os.Truncate("data/SSTable"+name+"/filter"+name+".txt", 0); err != nil {
		log.Printf("Failed to truncate: %v", err)
	}

	//Index
	if _, err := os.Stat("data/SSTable" + name + "/index" + name + ".txt"); err != nil {
		if os.IsNotExist(err) {
			_, err := os.Create("data/SSTable" + name + "/index" + name + ".txt")
			if err != nil {
				panic(err)
			}
		}
	}
	if err := os.Truncate("data/SSTable"+name+"/index"+name+".txt", 0); err != nil {
		log.Printf("Failed to truncate: %v", err)
	}

	//Summary
	if _, err := os.Stat("data/SSTable" + name + "/summary" + name + ".txt"); err != nil {
		if os.IsNotExist(err) {
			_, err := os.Create("data/SSTable" + name + "/summary" + name + ".txt")
			if err != nil {
				panic(err)
			}
		}
	}
	if err := os.Truncate("data/SSTable"+name+"/summary"+name+".txt", 0); err != nil {
		log.Printf("Failed to truncate: %v", err)
	}

	//SSTable
	if _, err := os.Stat("data/SSTable" + name + "/SSTable" + name + ".txt"); err != nil {
		if os.IsNotExist(err) {
			_, err := os.Create("data/SSTable" + name + "/SSTable" + name + ".txt")
			if err != nil {
				panic(err)
			}
		}
	}
	if err := os.Truncate("data/SSTable"+name+"/SSTable"+name+".txt", 0); err != nil {
		log.Printf("Failed to truncate: %v", err)
	}
}

func FindLastFile(level int) int {
	for j := 1; ; j++ {
		name := strconv.Itoa(level) + "_" + strconv.Itoa(j)
		if _, err := os.Stat("data/SSTable" + name); errors.Is(err, os.ErrNotExist) {
			return j
		}
	}
}
