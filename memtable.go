package main

import (
	"encoding/binary"
)

type Memtable struct {
	structure *SkipList
	max_size  int
	curr_size int
	threshold float64
}

//Funkcija kreira novu memtabelu
//Ideja je da se max_size i threshold uzimaju od nekih lokalnih promenljivih koje se podesavaju citanjem eksterne
//konfiguracije

func NewMemtable(max_s int, thresh float64) *Memtable {
	return &Memtable{
		structure: NewSkipList(),
		max_size:  max_s,
		threshold: thresh,
		curr_size: 0,
	}
}

//Funkcija za trazenje podatka u memtabeli po kljucu
//Vraca vrednost pridruzenu kljucu kao niz bajtova
func (m *Memtable) GetElement(key string) []byte {

	node := m.structure.GetElement(key)
	if node == nil {
		println("Nema zadatog kljuca u memtabeli!")
		return nil
	} else {
		key_size := binary.LittleEndian.Uint64(node.Input[13:21])
		value_size := binary.LittleEndian.Uint64((node.Input[21:29]))
		return node.Input[29+key_size : 29+key_size+value_size]
	}
}

/*Uzima podatak u formatu kao WAL i upisuje ga u memtabelu
Vraca status izvrsenja - true = uspesno, false = neuspesno (kljuc vec postoji u strukturi)*/
func (m *Memtable) PutElement(input []byte) (bool, error) {

	if m.structure.AddElement(input) == true {
		m.curr_size += 1
		return true, nil
	} else {
		//Menjamo vrednost elementa
		return false, nil
	}
}

/*Funkcija brise podatak pod zadatim kljucem iz memtabele
Vraca status izvrsenja*/
func (m *Memtable) DeleteElement(key string) bool {
	if m.DeleteElement(key) == true {
		return true
	} else {
		return false
	}
}

/*Funkcija prazni memtabelu i vraca sve zapise u memtabeli
Matrica bajtova se vraca zato sto svaki zapis u memtabeli predstavlja niz bajtova iste strukture kao WAL*/
func (m *Memtable) Flush() ([][]byte, error) {
	ret_val := m.structure.GetAll()
	m.curr_size = 0
	m.structure = NewSkipList()
	err := RecreateWAL()
	if err != nil {
		return ret_val, err
	}
	return ret_val, nil
}
