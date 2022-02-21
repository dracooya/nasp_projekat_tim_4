package main

import (
	"encoding/binary"
	"math/rand"
)


/*maxHeight se zadaje u kodu - nije u eksternom config -u
height je trenutna visina skip liste
size je ukupan broj elemenata u skip listi
head je prvi element skip liste*/
type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}


/*next je slice pokazivaca na sledeci element u skip listi - za svaki nivo na koji se propagira*/
type SkipListNode struct {

	Input 	  []byte		//zapisi u skiplisti su isti kao kod WAL-a!!!
	next      []*SkipListNode
}

/*Funkcija kreira novu praznu skiplistu
Interni dogovor - max visina skip liste je 32 - podrzava 2^32 vrednosti*/
func NewSkipList() *SkipList {

	in := make([]byte,30)
	binary.LittleEndian.PutUint64(in[13:21],1)	//key size praznog stringa je 1
	binary.LittleEndian.PutUint64(in[29:30],0)	//a vrednost 0
	//kada se konvertuje u string dobija se ""
	return &SkipList{
		maxHeight: 32,
		height: 0,
		size: 0,
		head: &SkipListNode{
			Input: in,
			next: make([]*SkipListNode,32),
		},
	}
}

/*Funkcija koja baca kockicu i odredjuje na koliko nivoa se propagira kljuc - vraca indeks maksimalnog nivoa na kome se element moze naci
Prima pokazivac na trenutnu skip listu*/
func (s *SkipList) roll() int {
	level := 0 // alwasy start from level 0

	// We roll until we don't get 1 from rand function and we did not
	// outgrow maxHeight. BUT rand can give us 0, and if that is the case
	// than we will just increase level, and wait for 1 from rand!
	for ; rand.Int31n(2) == 1 && level < s.maxHeight; level++ {
		if level > s.height {
			// When we get 1 from rand function and we did not
			// outgrow maxHeight, that number becomes new height
			s.height = level
			return level
		}
	}
	return level
}

/*Funkcija nalazi element sa zadatim kljucem u skip listi
Vraca SkipListNode sa zadatim kljucem
Vodi racuna o postavljenom tombstone - u!*/

func (s *SkipList) GetElement(key string) *SkipListNode{
	current_node := s.head
	for i := s.height; i >= 0; i-- {

		//pomeramo se udesno sve dok ne dodjemo do kraja trenutnog nivoa
		for ; current_node.next[i] != nil; current_node = current_node.next[i] {
			next_node := current_node.next[i]
			key_size := binary.LittleEndian.Uint64(current_node.Input[13:21])
			tombstone := int(current_node.Input[12])
			//Pronasli smo kljuc I NIJE OBRISAN
			if string(current_node.Input[29:29 + key_size]) == key && tombstone == 0{
				//println(i)
				return current_node
			}

			//Sledeci je trazeni
			next_key_size := binary.LittleEndian.Uint64(next_node.Input[13:21])
			next_tombstone := int(next_node.Input[12])
			//Sledeci cvor je trazeni - ubrzava algoritam za jednu iteraciju
			if string(next_node.Input[29:29 + next_key_size]) == key && next_tombstone == 0 {
				//println(i)
				return next_node
			}

			//Idemo dole
			if string(next_node.Input[29:29 + next_key_size]) > key{
				break
			}
		}
	}

	return nil
}

/*Funkcija dodaje element u skip listu
Prima niz bajtova u formatu kao i WAL*/
//Vraca status izvrsenja - true = uspesno, false = neuspesno
func (s *SkipList) AddElement(input []byte) bool {

	key_size := binary.LittleEndian.Uint64(input[13:21])
	key := string(input[29:29 + key_size])
	found := s.GetElement(key)

	//Element nije prethodno upisan u strukturu
	if found == nil {
		max_level := s.roll()
		//println(key + " se propagira do " + strconv.Itoa(max_level) + ". nivoa.")
		new_node := &SkipListNode{
			Input : input,
			next : make([]*SkipListNode,max_level + 1),
		}

		current := s.head

		/*Pocinjemo od najnizeg nivoa i penjemo se na poslednji nivo na kome treba da se nadje element*/
		for i := s.height; i >= 0; i-- {

			//Pomeramo se za jedno mesto udesno dok ne nadjemo poziciju
			for ; current.next[i] != nil; current = current.next[i] {
				next := current.next[i]
				next_key_size := binary.LittleEndian.Uint64(next.Input[13:21])
				if string(next.Input[29:29 + next_key_size]) > key { break }
			}

			if i > max_level {
				continue
			}

			new_node.next[i] = current.next[i]
			current.next[i] = new_node
		}
		s.size += 1
		return true
	} else {
		//Menjamo vrednost pod postojecim kljucem
		found.Input = input
		return true

	}
	return false
}

/*Funkcija brise element iz skip liste - postavlja tombstone podatka na 1
Vraca status izvrsenja*/
func (s *SkipList) DeleteElement(key string) bool{
	value := s.GetElement(key)
	if value != nil{
		value.Input[12] = 1	//tombstone postavljen
		return true

	} else{
		//println("Kljuc se ne nalazi u skiplisti!")
		return false
	}
}

/*Funkcija vraca sve elemente skip liste
Oni se svi nalaze na nultom nivou*/
func (s *SkipList) GetAll() [][]byte {
	current_node := s.head
	if current_node.next == nil {	//prazna skip lista
		return make([][]byte,0)
	} else{
		matrix := make([][]byte,s.size)
		for current_node = s.head.next[0]; current_node != nil; current_node = current_node.next[0] {
			matrix = append(matrix, current_node.Input)
		}

		return matrix
	}
}

/*Pomocna funkcija za iscrtavanje skip liste po nivoima*/
func (s *SkipList) printSkipList(){
	current_node := s.head
	for i := s.height; i >= 0; i-- {
		if s.head.next[i] == nil {
			continue
		}
		for current_node = s.head.next[i]; current_node != nil; current_node = current_node.next[i] {

			key_size := binary.LittleEndian.Uint64(current_node.Input[13:21])
			value_size := binary.LittleEndian.Uint64((current_node.Input[21:29]))
			tombstone := int (current_node.Input[12])
			if tombstone == 0 {
				print("(" + string(current_node.Input[29:29+key_size]) + "," + string(current_node.Input[29+key_size:29+key_size+value_size]) + ")  ")
			}
		}
		println()
	}
}

//staro testiranje
/*func main(){
	skip_list := newSkipList()
	skip_list.addElement("10",[]byte("idk1"))
	skip_list.addElement("8",[]byte("idk2"))
	skip_list.addElement("16",[]byte("idk3"))
	skip_list.addElement("9",[]byte("idk4"))
	skip_list.addElement("10",[]byte("idk5"))
	skip_list.printSkipList()
	skip_list.getElement("16")
}*/

