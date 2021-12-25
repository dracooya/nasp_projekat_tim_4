package writeaheadlog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

const (
	batchSize = 5
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type Entry struct {
	key       string
	value     []byte
	tombstone byte
	timestamp uint64
}

type Log struct {
	file  *os.File
	batch [][]byte
	i     int
}

func Create(path string) (*Log, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	log := &Log{file: file, batch: make([][]byte, batchSize), i: 0}
	return log, err
}

func Open(path string) (*Log, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	log := &Log{file: file, batch: make([][]byte, batchSize), i: 0}
	return log, err
}

func (log *Log) Close() error {
	err := log.writeBatch() // ako je nešto ostalo u bufferu, ispiši pre zatvaranja
	if err != nil {
		return err
	}
	err = log.file.Close()
	return err
}

func (log *Log) WritePutDirect(key string, value []byte) error {
	_, err := log.file.Seek(0, 2)
	if err != nil {
		return err
	}

	bytes := make([]byte, 29+len(key)+len(value))                              // 4+8+1+8+8 = 29 dužina jednog entry-a write ahead loga bez ključa i vrednosti
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
	_, err = log.file.Write(bytes)
	if err != nil {
		return err
	}
	err = log.file.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (log *Log) WritePutBuffer(key string, value []byte) error {
	bytes := make([]byte, 29+len(key)+len(value))                              // 4+8+1+8+8 = 29 dužina jednog entry-a write ahead loga bez ključa i vrednosti
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
	log.batch[log.i] = bytes
	log.i++
	if log.i == batchSize {
		err := log.writeBatch()
		if err != nil {
			return err
		}
		log.i = 0
		log.batch = make([][]byte, batchSize)
	}
	return nil
}

func (log *Log) WriteDeleteDirect(key string) error {
	_, err := log.file.Seek(0, 2)
	if err != nil {
		return err
	}

	bytes := make([]byte, 21+len(key))                                         // 4+8+1+8 = 21 dužina jednog entry-a write ahead loga bez ključa, vrednosti ali i value size-a jer je ovde nepotreban
	binary.LittleEndian.PutUint32(bytes[:4], CRC32([]byte(key)))               // CRC - 4B
	binary.LittleEndian.PutUint64(bytes[4:12], uint64(time.Now().UnixMicro())) // Timestamp - 8B
	bytes[12] = 1                                                              // Tombstone - 1B
	binary.LittleEndian.PutUint64(bytes[13:21], uint64(len(key)))              // Key size - 8B
	for i := 0; i < len(key); i++ {                                            // Key postavljen
		bytes[21+i] = key[i]
	}
	_, err = log.file.Write(bytes)
	if err != nil {
		return err
	}
	err = log.file.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (log *Log) WriteDeleteBuffer(key string) error {
	bytes := make([]byte, 21+len(key))                                         // 4+8+1+8 = 21 dužina jednog entry-a write ahead loga bez ključa, vrednosti ali i value size-a jer je ovde nepotreban
	binary.LittleEndian.PutUint32(bytes[:4], CRC32([]byte(key)))               // CRC - 4B
	binary.LittleEndian.PutUint64(bytes[4:12], uint64(time.Now().UnixMicro())) // Timestamp - 8B
	bytes[12] = 1                                                              // Tombstone - 1B
	binary.LittleEndian.PutUint64(bytes[13:21], uint64(len(key)))              // Key size - 8B
	for i := 0; i < len(key); i++ {                                            // Key postavljen
		bytes[21+i] = key[i]
	}
	log.batch[log.i] = bytes
	log.i++
	if log.i == batchSize {
		err := log.writeBatch()
		if err != nil {
			return err
		}
		log.i = 0
		log.batch = make([][]byte, batchSize)
	}
	return nil
}

func (log *Log) writeBatch() error {
	_, err := log.file.Seek(0, 2)
	if err != nil {
		return err
	}
	for i := 0; i < log.i; i++ {
		_, err = log.file.Write(log.batch[i])
		if err != nil {
			return err
		}
	}
	err = log.file.Sync()
	if err != nil {
		return err
	}
	return nil
}

func (log *Log) ReadAll() ([]Entry, error) {
	var entries []Entry

	_, err := log.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	for {
		entry := Entry{}
		var data = make([]byte, 21)

		_, err := log.file.Read(data)

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

			_, err = log.file.Read(data)
			if err != nil {
				return nil, err
			}

			valuesize := binary.LittleEndian.Uint64(data[:8])

			data = make([]byte, keysize+valuesize)

			_, err = log.file.Read(data)
			if err != nil {
				return nil, err
			}

			entry.key = string(data[:keysize])
			entry.value = data[keysize : keysize+valuesize]
		} else if entry.tombstone == 1 {
			data = make([]byte, keysize)

			_, err = log.file.Read(data)
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

	return entries, nil
}

func (log *Log) ReadAt(index int) (*Entry, error) {
	_, err := log.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	for i := 0; ; i++ {
		entry := Entry{}
		var data = make([]byte, 21)

		_, err := log.file.Read(data)

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

			_, err = log.file.Read(data)
			if err != nil {
				return nil, err
			}

			valuesize := binary.LittleEndian.Uint64(data[:8])

			data = make([]byte, keysize+valuesize)

			_, err = log.file.Read(data)
			if err != nil {
				return nil, err
			}

			entry.key = string(data[:keysize])
			entry.value = data[keysize : keysize+valuesize]
		} else if entry.tombstone == 1 {
			data = make([]byte, keysize)

			_, err = log.file.Read(data)
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

		if i == index {
			return &entry, nil
		}
	}

	return nil, errors.New("index out of bounds")
}

func test() {
	log, err := Create("wal")

	err = log.Close()

	if err != nil {
		fmt.Println(err)
		return
	}

	log, err = Open("wal")

	b := []byte{1, 2, 3, 2, 4, 6, 5, 7}
	err = log.WritePutDirect("test", b)

	if err != nil {
		fmt.Println(err)
		return
	}

	c := []byte{4, 3, 2, 1}
	err = log.WritePutDirect("test2", c)

	if err != nil {
		fmt.Println(err)
		return
	}

	err = log.WriteDeleteDirect("test")
	if err != nil {
		fmt.Println(err)
		return
	}

	all, err := log.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Ispis celog loga:", all)

	i1, err := log.ReadAt(0)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Ispis prvog entrya u logu:", i1)

	i2, err := log.ReadAt(1)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Ispis drugog entrya u logu:", i2)

	i3, err := log.ReadAt(2)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Ispis treceg entrya u logu:", i3)

	_, err = log.ReadAt(3)
	if err != nil {
		fmt.Println("Cetvrti entry:", err)
	}

	_ = log.WritePutBuffer("key1", b)
	_ = log.WritePutBuffer("key2", c)
	_ = log.WriteDeleteBuffer("key1")
	fmt.Println("Ispis celog loga tokom buffer dodavanja:", all)
	_ = log.WriteDeleteBuffer("key2")
	_ = log.WritePutBuffer("key3", c)
	_ = log.WriteDeleteBuffer("key3")

	all, err = log.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Ispis celog loga pre zatvaranja:", all)

	err = log.Close()
	if err != nil {
		fmt.Println(err)
		return
	}

	log, err = Open("wal")
	all, err = log.ReadAll()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Ispis celog loga posle zatvaranja:", all)
}
