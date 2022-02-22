package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"time"
)

type EntryWAL struct {
	key       string
	value     []byte
	tombstone byte
	timestamp uint64
}

type Log struct {
	endIndex  int
	currIndex int
	file      *os.File
	fileName  string
	batch     [][]byte
	batchNum  int
	entryNum  int
}

var (
	batchSize    = 4
	segmentSize  = 10
	lowWaterMark = 4

	ErrCorrupted   = errors.New("log corrupted")
	ErrOutOfBounds = errors.New("index out of bounds")
	ErrNotFound    = errors.New("file not found")

	log *Log
)

func fileLen(file *os.File) (int64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func mmapAppend(file *os.File, data []byte) error {
	currentLen, err := fileLen(file)
	if err != nil {
		return err
	}
	err = file.Truncate(currentLen + int64(len(data)))
	if err != nil {
		return err
	}
	mmapf, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return err
	}
	defer mmapf.Unmap()
	copy(mmapf[currentLen:], data)
	err = mmapf.Flush()
	if err != nil {
		return err
	}
	return nil
}

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

func Create(path string) (*Log, error) {
	err := os.Mkdir("wal", os.ModePerm)
	if err != nil {
		return nil, err
	}
	file, err := os.Create("wal/" + path + "_0")
	if err != nil {
		return nil, err
	}
	log := &Log{file: file, batch: make([][]byte, batchSize), batchNum: 0, endIndex: 0, currIndex: 0, fileName: path}
	return log, err
}

func Open(path string) (*Log, error) {
	files, err := os.ReadDir("wal/")
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, ErrNotFound
	}
	lastFile := files[0]
	for _, f := range files {
		if strings.HasPrefix(f.Name(), path+"_") {
			lastFile = f
		}
	}
	file, err := os.OpenFile("wal/"+lastFile.Name(), os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	index, err := strconv.Atoi(lastFile.Name()[len(path)+1:])
	if err != nil {
		return nil, err
	}
	entries, err := countEntries(file)
	if err != nil {
		return nil, err
	}
	log := &Log{file: file, batch: make([][]byte, batchSize), batchNum: 0, endIndex: index, currIndex: index, fileName: path, entryNum: entries}
	return log, nil
}

func (log *Log) Close() error {
	err := log.writeBatch() // ako je nešto ostalo u bufferu, ispiši pre zatvaranja
	if err != nil {
		return err
	}
	err = log.file.Close()
	if err != nil && !strings.Contains(err.Error(), fs.ErrClosed.Error()) {
		return err
	}
	return nil
}

func countEntries(file *os.File) (int, error) {
	for i := 0; ; i++ {
		_, err := file.Seek(12, 1)
		if err != nil {
			return -1, err
		}

		var data = make([]byte, 9)

		_, err = file.Read(data)

		if err != nil {
			if err == io.EOF {
				return i, nil
			}
			return -1, err
		}

		tombstone := data[0]
		keysize := binary.LittleEndian.Uint64(data[1:])

		if tombstone == 0 {
			data = make([]byte, 8)

			_, err = file.Read(data)
			if err != nil {
				return -1, err
			}

			valuesize := binary.LittleEndian.Uint64(data)

			data = make([]byte, keysize+valuesize)

			_, err := file.Seek(int64(keysize+valuesize), 1)
			if err != nil {
				return -1, err
			}
		} else if tombstone == 1 {
			_, err := file.Seek(int64(keysize), 1)
			if err != nil {
				return -1, err
			}
		} else {
			return -1, ErrCorrupted
		}
	}
}

func FormBytesPut(key string, value []byte) []byte {
	bytes := make([]byte, 29+len(key)+len(value))                              // 4+8+1+8+8 = 29 dužina jednog entry-a write ahead loga bez ključa batchNum vrednosti
	binary.LittleEndian.PutUint32(bytes[:4], CRC32([]byte(key)))               // CRC - 4B
	binary.LittleEndian.PutUint64(bytes[4:12], uint64(time.Now().UnixMicro())) // Timestamp - 8B
	bytes[12] = 0                                                              // Tombstone - 1B
	binary.LittleEndian.PutUint64(bytes[13:21], uint64(len(key)))              // Key size - 8B
	binary.LittleEndian.PutUint64(bytes[21:29], uint64(len(value)))            // Value size - 8B
	for i := 0; i < len(key); i++ {                                            // Key postavljen
		bytes[29+i] = key[i]
	}
	for i := 0; i < len(value); i++ { // Value postavljen
		bytes[29+len(key)+i] = value[i]
	}

	return bytes
}

func FormBytesDelete(key string) []byte {
	bytes := make([]byte, 21+len(key))                                         // 4+8+1+8 = 21 dužina jednog entry-a write ahead loga bez ključa, vrednosti ali batchNum value size-a jer je ovde nepotreban
	binary.LittleEndian.PutUint32(bytes[:4], CRC32([]byte(key)))               // CRC - 4B
	binary.LittleEndian.PutUint64(bytes[4:12], uint64(time.Now().UnixMicro())) // Timestamp - 8B
	bytes[12] = 1                                                              // Tombstone - 1B
	binary.LittleEndian.PutUint64(bytes[13:21], uint64(len(key)))              // Key size - 8B
	for i := 0; i < len(key); i++ {                                            // Key postavljen
		bytes[21+i] = key[i]
	}

	return bytes
}

func (log *Log) checkWaterMark() error {
	if log.endIndex > lowWaterMark {
		err := log.file.Close()
		if err != nil && !strings.Contains(err.Error(), fs.ErrClosed.Error()) {
			return err
		}
		files, err := os.ReadDir("wal/")
		if err != nil {
			return err
		}
		var filesForDeletion []string

		for _, f := range files {
			if strings.HasPrefix(f.Name(), log.fileName+"_") {
				filesForDeletion = append(filesForDeletion, "wal/"+f.Name())
			}
		}
		for _, f := range filesForDeletion {
			err := os.Remove(f)
			if err != nil {
				return err
			}
		}
		log.endIndex = 0
	}
	return nil
}

func (log *Log) WritePutDirect(key string, value []byte) error {
	err := log.activateLastSegment()
	if err != nil {
		return err
	}
	if log.entryNum < segmentSize {
		err := log.writePutDirect(key, value)
		if err != nil {
			return err
		}
		log.entryNum++
	} else {
		log.endIndex++

		err := log.checkWaterMark()
		if err != nil {
			return err
		}

		log.currIndex = log.endIndex
		log.entryNum = 0
		file, err := os.Create("wal/" + log.fileName + "_" + strconv.Itoa(log.currIndex))
		if err != nil {
			return err
		}
		if log.file != nil {
			err := log.file.Close()
			if err != nil && !strings.Contains(err.Error(), fs.ErrClosed.Error()) {
				return err
			}
		}
		log.file = file
		err = log.writePutDirect(key, value)
		if err != nil {
			return err
		}
		log.entryNum++
	}
	return nil
}

func (log *Log) writePutDirect(key string, value []byte) error {
	bytes := FormBytesPut(key, value)

	err := mmapAppend(log.file, bytes)
	if err != nil {
		return err
	}

	return nil
}

func (log *Log) WritePutBuffer(key string, value []byte) (error,[]byte) {
	bytes := FormBytesPut(key, value)
	err := log.writeBuffer(bytes)
	if err != nil {
		return err,nil
	}
	return nil,bytes
}

func (log *Log) WriteDeleteDirect(key string) error {
	err := log.activateLastSegment()
	if err != nil {
		return err
	}
	if log.entryNum < segmentSize {
		err := log.writeDeleteDirect(key)
		if err != nil {
			return err
		}
		log.entryNum++
	} else {
		log.endIndex++

		err := log.checkWaterMark()
		if err != nil {
			return err
		}

		log.currIndex = log.endIndex
		log.entryNum = 0
		file, err := os.Create("wal/" + log.fileName + "_" + strconv.Itoa(log.currIndex))
		if err != nil {
			return err
		}
		if log.file != nil {
			err := log.file.Close()
			if err != nil && !strings.Contains(err.Error(), fs.ErrClosed.Error()) {
				return err
			}
		}
		log.file = file
		err = log.writeDeleteDirect(key)
		if err != nil {
			return err
		}
		log.entryNum++
	}
	return nil
}

func (log *Log) writeDeleteDirect(key string) error {
	bytes := FormBytesDelete(key)

	err := mmapAppend(log.file, bytes)
	if err != nil {
		return err
	}

	return nil
}

func (log *Log) WriteDeleteBuffer(key string) error {
	bytes := FormBytesDelete(key)
	err := log.writeBuffer(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (log *Log) writeBuffer(bytes []byte) error {
	log.batch[log.batchNum] = bytes
	log.batchNum++
	if log.batchNum == batchSize {
		err := log.writeBatch()
		if err != nil {
			return err
		}
		log.batchNum = 0
		log.batch = make([][]byte, batchSize)
	}
	return nil
}

func (log *Log) activateLastSegment() error {
	if log.currIndex != log.endIndex {
		log.currIndex = log.endIndex
		if log.file != nil {
			err := log.file.Close()
			if err != nil && !strings.Contains(err.Error(), fs.ErrClosed.Error()) {
				return err
			}
		}
		file, err := os.Open("wal/" + log.fileName + "_" + strconv.Itoa(log.currIndex))
		if err != nil {
			return err
		}
		entries, err := countEntries(file)
		if err != nil {
			return err
		}
		log.entryNum = entries
		log.file = file
	}
	return nil
}

func (log *Log) writeBatch() error {
	err := log.activateLastSegment()
	if err != nil {
		return err
	}

	_, err = log.file.Seek(0, 2)
	if err != nil {
		return err
	}

	for i := 0; i < log.batchNum; i++ {
		if log.entryNum < segmentSize {
			err := mmapAppend(log.file, log.batch[i])
			if err != nil {
				return err
			}
			log.entryNum++
		} else {
			log.endIndex++
			err := log.checkWaterMark()
			if err != nil {
				return err
			}
			log.currIndex = log.endIndex
			log.entryNum = 0
			file, err := os.Create("wal/" + log.fileName + "_" + strconv.Itoa(log.currIndex))
			if err != nil {
				return err
			}
			if log.file != nil {
				err := log.file.Close()
				if err != nil && !strings.Contains(err.Error(), fs.ErrClosed.Error()) {
					return err
				}
			}
			log.file = file
			err = mmapAppend(log.file, log.batch[i])
			if err != nil {
				return err
			}
			log.entryNum++
		}
	}
	return nil
}

func (log *Log) ReadAll() ([]EntryWAL, error) {
	var entries []EntryWAL

	for i := 0; i <= log.endIndex; i++ {
		file, err := os.Open("wal/" + log.fileName + "_" + strconv.Itoa(i))
		defer file.Close()
		if err != nil {
			return entries, err
		}

		for {
			entry := EntryWAL{}

			var data = make([]byte, 21)

			_, err = file.Read(data)

			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}

			crc := binary.LittleEndian.Uint32(data[:4])
			entry.timestamp = binary.LittleEndian.Uint64(data[4:12])
			entry.tombstone = data[12]
			keysize := binary.LittleEndian.Uint64(data[13:21])

			if entry.tombstone == 0 {
				data = make([]byte, 8)

				_, err = file.Read(data)
				if err != nil {
					return nil, err
				}

				valuesize := binary.LittleEndian.Uint64(data[:8])

				data = make([]byte, keysize+valuesize)

				_, err = file.Read(data)
				if err != nil {
					return nil, err
				}

				entry.key = string(data[:keysize])
				entry.value = data[keysize : keysize+valuesize]
			} else if entry.tombstone == 1 {
				data = make([]byte, keysize)

				_, err = file.Read(data)
				if err != nil {
					return nil, err
				}

				entry.key = string(data[:])
			} else {
				return nil, ErrCorrupted
			}

			if CRC32([]byte(entry.key)) != crc {
				return nil, ErrCorrupted
			}

			entries = append(entries, entry)
		}
	}

	return entries, nil
}

func (log *Log) ReadAt(index int) (*EntryWAL, error) {
	j := 0
	for i := 0; i <= log.endIndex; i++ {
		file, err := os.Open("wal/" + log.fileName + "_" + strconv.Itoa(i))
		defer file.Close()
		if err != nil {
			return nil, err
		}

		for {
			if j != index {
				_, err := file.Seek(12, 1)
				if err != nil {
					return nil, err
				}

				var data = make([]byte, 9)

				_, err = file.Read(data)

				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}

				tombstone := data[0]
				keysize := binary.LittleEndian.Uint64(data[1:])

				if tombstone == 0 {
					data = make([]byte, 8)

					_, err = file.Read(data)
					if err != nil {
						return nil, err
					}

					valuesize := binary.LittleEndian.Uint64(data)

					_, err := file.Seek(int64(keysize+valuesize), 1)
					if err != nil {
						return nil, err
					}
				} else if tombstone == 1 {
					_, err := file.Seek(int64(keysize), 1)
					if err != nil {
						return nil, err
					}
				} else {
					return nil, ErrCorrupted
				}
			} else {
				entry := EntryWAL{}

				var data = make([]byte, 21)

				_, err = file.Read(data)

				if err != nil {
					if err == io.EOF {
						break
					}
					return nil, err
				}

				crc := binary.LittleEndian.Uint32(data[:4])
				entry.timestamp = binary.LittleEndian.Uint64(data[4:12])
				entry.tombstone = data[12]
				keysize := binary.LittleEndian.Uint64(data[13:21])

				if entry.tombstone == 0 {
					data = make([]byte, 8)

					_, err = file.Read(data)
					if err != nil {
						return nil, err
					}

					valuesize := binary.LittleEndian.Uint64(data[:8])

					data = make([]byte, keysize+valuesize)

					_, err = file.Read(data)
					if err != nil {
						return nil, err
					}

					entry.key = string(data[:keysize])
					entry.value = data[keysize : keysize+valuesize]
				} else if entry.tombstone == 1 {
					data = make([]byte, keysize)

					_, err = file.Read(data)
					if err != nil {
						return nil, err
					}

					entry.key = string(data[:])
				} else {
					return nil, ErrCorrupted
				}

				if CRC32([]byte(entry.key)) != crc {
					return nil, ErrCorrupted
				}

				return &entry, nil
			}
			j++
		}
	}

	return nil, ErrOutOfBounds
}

// ClearWALFolder - funkcija koja cisti folder koji sadrzi sve WAL segmente
func ClearWALFolder() {
	err := os.RemoveAll("wal/")
	if err != nil {
		return
	}
}

// LoadConfigurations - funkcija koja ucitava sve inicijalne konfiguracije wal-a
func LoadConfigurations() error {
	config, err := os.Open("config.txt")
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(config)

	scanner.Scan()
	batchSize, err = strconv.Atoi(scanner.Text()[10:])
	if err != nil {
		return err
	}
	scanner.Scan()
	segmentSize, err = strconv.Atoi(scanner.Text()[12:])
	if err != nil {
		return err
	}
	scanner.Scan()
	lowWaterMark, err = strconv.Atoi(scanner.Text()[13:])
	if err != nil {
		return err
	}

	err = config.Close()
	if err != nil {
		return err
	}
	return nil
}

// InitializeWALConfigs - funkcija koja inicijalizuje konfiguracije potrebne za funkcionisanje wal-a
func InitializeWALConfigs(batchSize_ int, segmentSize_ int, lowWaterMark_ int) {
	batchSize = batchSize_
	segmentSize = segmentSize_
	lowWaterMark = lowWaterMark_
}

// InitWAL - funkcija koja inicijalizuje novi WAL
func InitWAL() error {
	ClearWALFolder()
	var err error
	log, err = Create("wal")
	if err != nil {
		return err
	}
	return nil
}

// RecreateWAL - funkcija koja brise WAL i kreira novi
func RecreateWAL() error {
	err := log.Close()
	if err != nil {
		return err
	}
	err = InitWAL()
	if err != nil {
		return err
	}
	return nil
}

func test() {
	/*err := LoadConfigurations() // podešavanja se sada učitavaju u mainu
	if os.IsNotExist(err) {
		fmt.Println("config.txt ne postoji, u upotrebi su podrazumevana podešavanja.")
	}*/

	ClearWALFolder()

	var err error
	log, err = Create("wal")
	if err != nil {
		fmt.Println(err)
		return
	}

	err = log.WritePutDirect("a", []byte{1, 2, 3})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = log.WritePutDirect("b", []byte{4, 5, 6, 7})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = log.WritePutDirect("c", []byte{9, 2, 3})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = log.WritePutDirect("d", []byte{2, 4, 3})
	if err != nil {
		fmt.Println(err)
		return
	}

	err = log.WriteDeleteDirect("a")
	if err != nil {
		fmt.Println(err)
		return
	}

	err = log.WriteDeleteDirect("b")
	if err != nil {
		fmt.Println(err)
		return
	}

	err = log.WriteDeleteDirect("c")
	if err != nil {
		fmt.Println(err)
		return
	}

	all, err := log.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(all)

	err = log.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	log, err = Open("wal")
	if err != nil {
		fmt.Println(err)
		return
	}

	err,_ = log.WritePutBuffer("c", []byte{10, 12, 13, 15})
	if err != nil {
		fmt.Println(err)
		return
	}
	err,_ = log.WritePutBuffer("d", []byte{1, 12})
	if err != nil {
		fmt.Println(err)
		return
	}

	err = log.WriteDeleteBuffer("c")
	if err != nil {
		fmt.Println(err)
		return
	}
	err = log.WriteDeleteBuffer("d")
	if err != nil {
		fmt.Println(err)
		return
	}

	all, err = log.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(all)

	err = log.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	log, err = Open("wal")
	if err != nil {
		fmt.Println(err)
		return
	}

	all, err = log.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(all)
	err = log.WriteDeleteBuffer("c")
	if err != nil {
		fmt.Println(err)
		return
	}
	err,_ = log.WritePutBuffer("c", []byte{10, 12, 13, 15})
	if err != nil {
		fmt.Println(err)
		return
	}
	err,_ = log.WritePutBuffer("d", []byte{1, 12})
	if err != nil {
		fmt.Println(err)
		return
	}
	all, err = log.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(all)

	at, err := log.ReadAt(0)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(at)
	at, err = log.ReadAt(2)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(at)
	at, err = log.ReadAt(4)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(at)
	at, err = log.ReadAt(5)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(at)
	at, err = log.ReadAt(6)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(at)
	at, err = log.ReadAt(7)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(at)
	at, err = log.ReadAt(12)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(at)
	at, err = log.ReadAt(13)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(at)

	_, err = log.ReadAt(14)
	if err != nil {
		fmt.Println(err)
	}

	err = log.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
}
