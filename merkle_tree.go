package main

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"io/ioutil"
	"os"
)

/*TODO:Prilagoditi sve ka Data segment formatu*/
type Data struct{
	/*crc [4]byte
	timestamp [8]byte
	tombstone [1]byte
	value_size [8]byte
	value []byte*/
	value string
}

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string{
	return mr.root.String()
}

/*Ukoliko je data prazan niz bajtova -> Node je prazan, tj. dodaje se zbog kompletnosti stabla*/
/*data cuva hash vrednosti!*/
type Node struct {
	data [20]byte
	left *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

/*U slucaju da se hesiraju cvorovi koji nisu listovi*/
func hash_pairs(node_1 *Node ,node_2 *Node) [20]byte{
	right_hash := []byte {}
	if len(node_2.data) != 0 {
		right_hash = node_2.data[:]
	}
	return Hash(append(node_1.data[:],right_hash[:]...))
}

/*Rekurzivno boottom-up konstruisanje merkle stabla */
func BuildMerkle(data []Node) *MerkleRoot {
	var nodes []Node
	for i :=  0; i < len(data); i+=2 {

		left_node := data[i]
		var right_node = Node{}
		if (i + 1) < len(data) {
			right_node = data[i+1]
		} else {
			right_node = Node{data: [20]byte{}, left: nil, right: nil}
		}
		nodes = append(nodes, Node{left: &left_node, right: &right_node, data: hash_pairs(&left_node, &right_node)})
	}
	if len(nodes) == 1{
		return &MerkleRoot{&nodes[0]}
	}else{
		return BuildMerkle(nodes)
	}
}
/*Funkcija uzima pocetne blokove podataka i pretvara ih u listove*/
func toNodeList(data []Data) []Node{
	var node_list = []Node{}
	for _,elem := range data{
		node_list = append(node_list,Node{left:nil,right:nil,data:Hash([]byte(elem.value))})
	}
	return node_list
}

/*Breadth first obilazak stabla pocevsi od njegovog korena i pretvaranja stabla u niz hash vrednosti*/
func treeToList(root *Node) [][20]byte{
	list := [][20]byte {}
	var queue = []*Node{root}
	for true{
		if len(queue) == 0{
			break
		}
		root = queue[0]
		list = append(list, root.data)
		queue = queue[1:]

		if root.left != nil{
			queue = append(queue, root.left)
		}

		if root.right != nil{
			queue = append(queue, root.right)
		}

	}

	return list
}

/*Serijalizacija stabla u datoteku
file_name -> target datoteka
tree_list -> breadth - first obidjeno merkle stablo*/
func serialize(tree_list [][20]byte,file_name string){
	file,err := os.OpenFile(file_name,os.O_CREATE|os.O_WRONLY,0777)
	if err != nil{
		panic(err.Error())
	}
	defer file.Close()
	error := binary.Write(file, binary.LittleEndian, tree_list)
	if error != nil{
		panic(error.Error())
	}
}

/*Deserijalizacija stabla is target file_name datoteke
Sluzi kao provera*/
func deserialize(file_name string) [][]byte{
	var result_list [][]byte
	file,err := os.OpenFile(file_name,os.O_RDONLY,0777)
	if err != nil{
		panic(err.Error())
	}
	defer file.Close()
	data,error := ioutil.ReadAll(file)
	if error != nil{
		panic(error.Error())
	}

	for i:= 0; i < len(data)/20;i++{
		result_list = append(result_list,data[i * 20:i * 20 + 20])
	}
	return result_list
}

func main(){
	var podaci = []Data{Data{value:"a"},Data{value:"b"},Data{value:"c"},Data{value:"d"}}
	var node_list = toNodeList(podaci)
	var mr = BuildMerkle(node_list)
	var tree_list = treeToList(mr.root)
	serialize(tree_list,"Metadata.txt")
	var result = deserialize("Metadata.txt")

	for _,value := range result{
		println(hex.EncodeToString(value))
	}
}