package summary

import (
	"encoding/binary"
	"log"
	"os"
)

func NewSummary(keys []string, keyLen []uint64, name string) {
	file, err := os.OpenFile("data/SSTable"+name+"/summary"+name+".txt", os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	bytes := make([]byte, 8)
	for i, key := range keys {
		binary.LittleEndian.PutUint64(bytes, keyLen[i])
		_, err := file.Write(bytes) //key len
		if err != nil {
			log.Fatal(err)
		}
		_, err = file.Write([]byte(key)) //key
		if err != nil {
			log.Fatal(err)
		}
		binary.LittleEndian.PutUint64(bytes, uint64(i*8))
		_, err = file.Write(bytes) //Offset in index
		if err != nil {
			log.Fatal(err)
		}
	}
}

func Find(key string, name string) (uint64, bool) {
	file, err := os.Open("data/SSTable" + name + "/summary" + name + ".txt")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	offset := uint64(0)
	found := false
	for {
		bytes := make([]byte, 8)
		_, err = file.Read(bytes)
		if err != nil {
			break //EOF
		}
		keyLen := binary.LittleEndian.Uint64(bytes)
		readKey := make([]byte, keyLen)
		file.Read(readKey)
		file.Read(bytes)
		if string(readKey) == key {
			offset = binary.LittleEndian.Uint64(bytes)
			found = true
			break
		}
	}
	return uint64(offset), found
}
