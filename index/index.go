package index

import (
	"encoding/binary"
	"log"
	"os"
)

//entrys - Duzine kljuceva i vrednosti
func NewIndex(entrysLen []uint64, name string) {

	//Ako je potrebno napraviti novi index file

	file, err := os.OpenFile("data/SSTable"+name+"/index"+name+".txt", os.O_WRONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	//Upis svih offseta u index
	bytes := make([]byte, 8)
	offset := uint64(0)
	for i := 0; i < len(entrysLen); i++ {
		binary.LittleEndian.PutUint64(bytes, offset)
		_, err = file.Write(bytes)
		if err != nil {
			log.Fatal(err)
		}
		offset += entrysLen[i]
	}
}

func Find(offset uint64, name string) uint64 {
	file, err := os.Open("data/SSTable" + name + "/index" + name + ".txt")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	tableOffset := make([]byte, 8)
	file.Seek(int64(offset), 0)
	file.Read(tableOffset)
	return binary.LittleEndian.Uint64(tableOffset)
}
