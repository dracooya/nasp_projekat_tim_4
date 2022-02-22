package kompakcije

import (
	"encoding/binary"
	"errors"
	"log"
	"main/SSTable"
	"os"
	"strconv"
)

type entry struct {
	date      uint64
	key       string
	value     []byte
	tombstone bool
	loaded    bool
}

func Kompakcija(merge int, maxLevel int,bloomPer float64) {
	//1 iteration for every level
	for level := 1; level < maxLevel; level++ {
		last := FindLastFile(level)
		//Try to merge until can't
		for next := true; next; {
			next = mergeTables(merge, level,bloomPer)
			//Delete merged tables
			if next {
				tidyLevel(level, merge, last)
			}
		}
	}
}

func mergeTables(merge int, level int,bloomPer float64) bool {
	//Slice of files to merge
	files, err := loadTables(merge, level)
	if err {
		return false
	}

	//Data for the new table
	newTableData := fillData(files)
	closeFiles(files)
	SSTable.MakeTable(newTableData, level+1,bloomPer)
	return true
}

func fillData(files []*os.File) [][]byte {
	merge := len(files)
	newTableData := make([][]byte, 0)

	//read from every loaded file and fill initial entrys
	//initial fill entrys
	entrys := make([]entry, merge)
	for i := 0; i < merge; i++ {
		entrys[i] = readEntry(files[i])
	}

	//iter until end
	for next := true; next; {
		//best entry, written inserted into newTableData
		//load is index of file of best
		best, load := findBest(entrys)

		newTableData = append(newTableData, best.value)
		entrys[load] = readEntry(files[load])

		//check if there are entrys left in any file
		someLoaded := true
		for i := 0; i < merge; i++ {
			if entrys[i].loaded {
				someLoaded = true
				break
			} else {
				someLoaded = false
			}
		}
		next = someLoaded
	}
	return newTableData
}

func loadTables(merge int, level int) ([]*os.File, bool) {
	files := make([]*os.File, merge)
	//fill files
	i := 0
	for table := 1; table <= merge; table++ {
		name := strconv.Itoa(level) + "_" + strconv.Itoa(table)
		tempFile, err := os.OpenFile("data/SSTable"+name+"/SSTable"+name+".txt", os.O_RDONLY, 0666)
		//if file can't be opened -> no more needed to merge
		if err != nil {
			tempFile.Close()
			return files, true
		}
		files[i] = tempFile
		i++
	}
	return files, false
}

func tidyLevel(level int, merge int, lastTable int) {
	for i := 1; i <= merge; i++ {
		name := strconv.Itoa(level) + "_" + strconv.Itoa(i)
		err := os.RemoveAll("data/SSTable" + name)
		if err != nil {
			log.Fatal(err)
		}
	}
	for i := merge + 1; ; i++ {
		name := strconv.Itoa(level) + "_" + strconv.Itoa(i)
		if _, err := os.Stat("data/SSTable" + name); errors.Is(err, os.ErrNotExist) {
			break
		}
		newName := strconv.Itoa(level) + "_" + strconv.Itoa(i-merge)
		//rename directory
		err := os.Rename("data/SSTable"+name, "data/SSTable"+newName)
		if err != nil {
			log.Fatal(err)
		}
		//rename SSTable
		err = os.Rename("data/SSTable"+newName+"/SSTable"+name+".txt", "data/SSTable"+newName+"/SSTable"+newName+".txt")
		if err != nil {
			log.Fatal(err)
		}
		//rename filter
		err = os.Rename("data/SSTable"+newName+"/filter"+name+".txt", "data/SSTable"+newName+"/filter"+newName+".txt")
		if err != nil {
			log.Fatal(err)
		}
		//rename index
		err = os.Rename("data/SSTable"+newName+"/index"+name+".txt", "data/SSTable"+newName+"/index"+newName+".txt")
		if err != nil {
			log.Fatal(err)
		}
		//rename summary
		err = os.Rename("data/SSTable"+newName+"/summary"+name+".txt", "data/SSTable"+newName+"/summary"+newName+".txt")
		if err != nil {
			log.Fatal(err)
		}
		//rename merk
		err = os.Rename("data/SSTable"+newName+"/metadata"+name+".txt", "data/SSTable"+newName+"/metadata"+newName+".txt")
		if err != nil {
			log.Fatal(err)
		}

	}
}

func readEntry(file *os.File) entry {
	var ret entry
	ret.loaded = true
	ret.value = make([]byte, 0)
	bytes := make([]byte, 4)
	_, err := file.Read(bytes)
	if err != nil {
		ret.loaded = false
		return ret
	}
	ret.value = append(ret.value, bytes...)
	bytes = make([]byte, 8)
	file.Read(bytes)
	ret.value = append(ret.value, bytes...)
	ret.date = binary.LittleEndian.Uint64(bytes)
	bytes = make([]byte, 1)
	file.Read(bytes)
	ret.value = append(ret.value, bytes...)
	if int(bytes[0]) == 0 {
		ret.tombstone = false
	} else {
		ret.tombstone = true
	}
	bytes = make([]byte, 8)
	file.Read(bytes)
	ret.value = append(ret.value, bytes...)
	keySize := binary.LittleEndian.Uint64(bytes)
	file.Read(bytes)
	ret.value = append(ret.value, bytes...)
	valueSize := binary.LittleEndian.Uint64(bytes)
	bytes = make([]byte, keySize)
	file.Read(bytes)
	ret.value = append(ret.value, bytes...)
	ret.key = string(bytes)
	bytes = make([]byte, valueSize)
	file.Read(bytes)
	ret.value = append(ret.value, bytes...)
	return ret
}

func findBest(entrys []entry) (entry, int) {
	var best entry
	merge := len(entrys)
	best.date = entrys[0].date
	best.value = entrys[0].value
	//from witch table load new entry
	load := 0
	for i := 1; i < merge; i++ {
		if entrys[i].loaded {
			if best.key < entrys[i].key {
				if best.key == entrys[i].key {
					if best.date < entrys[i].date {
						best.value = entrys[i].value
						best.date = entrys[i].date
						load = i
					}
				} else {
					best.value = entrys[i].value
					best.date = entrys[i].date
					load = i
				}
			}
		}
	}
	return best, load
}

func closeFiles(files []*os.File) {
	for _, file := range files {
		err:= file.Close()
		if err != nil{
			//println("nesto")
			panic(err)
		}
	}
}

func FindLastFile(level int) int {
	for j := 1; ; j++ {
		name := strconv.Itoa(level) + "_" + strconv.Itoa(j)
		if _, err := os.Stat("data/SSTable" + name); errors.Is(err, os.ErrNotExist) {
			return j - 1
		}
	}
}
