package writeaheadlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	batchSize   = 3
	segmentSize = 6
)

type Entry struct {
	key       string
	value     []byte
	tombstone byte
	timestamp uint64
}

type Log struct {
	startIndex int
	endIndex   int
	currIndex  int
	file       *os.File
	fileName   string
	batch      [][]byte
	batchNum   int
	entryNum   int
}

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
	log := &Log{file: file, batch: make([][]byte, batchSize), batchNum: 0, startIndex: 0, endIndex: 0, currIndex: 0, fileName: path}
	return log, err
}

func Open(path string) (*Log, error) {
	files, err := os.ReadDir("wal/")
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, errors.New("fajl sa ovim imenom ne postoji")
	}
	lastFile := files[0]
	for _, f := range files {
		if strings.HasPrefix(f.Name(), path) {
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
	log := &Log{file: file, batch: make([][]byte, batchSize), batchNum: 0, startIndex: 0, endIndex: index, currIndex: index, fileName: path, entryNum: entries}
	return log, nil
}

func (log *Log) Close() error {
	err := log.writeBatch() // ako je nešto ostalo u bufferu, ispiši pre zatvaranja
	if err != nil {
		return err
	}
	err = log.file.Close()
	return err
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
			return -1, errors.New("log corrupted")
		}
	}
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
		log.currIndex = log.endIndex
		log.entryNum = 0
		file, err := os.Create("wal/" + log.fileName + "_" + strconv.Itoa(log.currIndex))
		if err != nil {
			return err
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
	bytes := formBytesPut(key, value)

	err := mmapAppend(log.file, bytes)
	if err != nil {
		return err
	}

	return nil
}

func formBytesPut(key string, value []byte) []byte {
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

func (log *Log) WritePutBuffer(key string, value []byte) error {
	bytes := formBytesPut(key, value)
	err := log.writeBuffer(bytes)
	if err != nil {
		return err
	}
	return nil
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
		log.currIndex = log.endIndex
		log.entryNum = 0
		file, err := os.Create("wal/" + log.fileName + "_" + strconv.Itoa(log.currIndex))
		if err != nil {
			return err
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
	bytes := formBytesDelete(key)

	err := mmapAppend(log.file, bytes)
	if err != nil {
		return err
	}

	return nil
}

func (log *Log) WriteDeleteBuffer(key string) error {
	bytes := formBytesDelete(key)
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

func formBytesDelete(key string) []byte {
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

func (log *Log) activateLastSegment() error {
	if log.currIndex != log.endIndex {
		log.currIndex = log.endIndex
		if log.file != nil {
			err := log.file.Close()
			if err != nil {
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
			log.currIndex = log.endIndex
			log.entryNum = 0
			file, err := os.Create("wal/" + log.fileName + "_" + strconv.Itoa(log.currIndex))
			if err != nil {
				return err
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

func (log *Log) ReadAll() ([]Entry, error) {
	var entries []Entry

	for i := log.startIndex; i <= log.endIndex; i++ {
		file, err := os.Open("wal/" + log.fileName + "_" + strconv.Itoa(i))
		if err != nil {
			return entries, err
		}

		for {
			entry := Entry{}

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
				return nil, errors.New("log corrupted")
			}

			if CRC32([]byte(entry.key)) != crc {
				return nil, errors.New("log corrupted")
			}

			entries = append(entries, entry)
		}
	}

	return entries, nil
}

func (log *Log) ReadAt(index int) (*Entry, error) {
	j := 0
	for i := log.startIndex; i <= log.endIndex; i++ {
		file, err := os.Open("wal/" + log.fileName + "_" + strconv.Itoa(i))
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
					return nil, errors.New("log corrupted")
				}
			} else {
				entry := Entry{}

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
					return nil, errors.New("log corrupted")
				}

				if CRC32([]byte(entry.key)) != crc {
					return nil, errors.New("log corrupted")
				}

				return &entry, nil
			}
			j++
		}
	}

	return nil, errors.New("index out of bounds")
}

func ClearWALFolder() {
	err := os.RemoveAll("wal/")
	if err != nil {
		return
	}
}

func test() {
	ClearWALFolder()

	log, err := Create("wal")
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

	err = log.writeDeleteDirect("a")
	if err != nil {
		fmt.Println(err)
		return
	}
	err = log.writeDeleteDirect("b")
	if err != nil {
		fmt.Println(err)
		return
	}
	err = log.writeDeleteDirect("c")
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

	err = log.WritePutBuffer("c", []byte{10, 12, 13, 15})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = log.WritePutBuffer("d", []byte{1, 12})
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

	err = log.WritePutBuffer("c", []byte{10, 12, 13, 15})
	if err != nil {
		fmt.Println(err)
		return
	}
	err = log.WritePutBuffer("d", []byte{1, 12})
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
