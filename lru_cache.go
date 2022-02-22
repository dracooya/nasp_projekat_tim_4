package main

import (
	"container/list"
)

/*cache_list je dvostruko spregnuta lista u kojoj se nalaze elementi cache-a
cache_map omogucava O(1) pristup
cache_limit je max broj stavki u cache-u*/
type Cache struct{
	 cache_list *list.List
	 cache_map map[string]*list.Element
	 cache_limit int

}

/*Model podatka u dvostruko spregnutoj listi*/
type KV struct {
	key string
	value []byte
}

/*Kreiranje novog kesa
limit je procitan podatak iz eksterne konfiguracije koji predstavlja max broj stavki u cache-u*/
func createCache(limit int) *Cache {
	return &Cache{cache_list: list.New(),cache_map: make(map[string]*list.Element),cache_limit: limit}
}

/*Trazenje stavke u cache-u
prosledjuje se kljuc koji se trazi*/
func (cache *Cache) Search(key string) *list.Element {
	value,is_present := cache.cache_map[key]

	if !is_present{
		return nil
	} else{
		cache.cache_list.MoveToBack(value)
		return value
	}
}

/*Pomocna funkcija za testiranje
Ispisuje se ceo sadrzaj cache-a
Prosledjuje se prethodno kreiran cache objekat*/
func (cache *Cache) printCache(){
	for e := cache.cache_list.Front(); e != nil; e = e.Next() {
		print(string(e.Value.(KV).value )+ " ")

	}
	println()
}


/*Dodavanje stavke u cache
Prosledjuje se kljuc sa njemu pridruzenom vrednoscu*/
func (cache *Cache) Insert(key string,value []byte){
	found := cache.Search(key)
	if found == nil{
		if cache.cache_list.Len() == cache.cache_limit{
			lru := cache.cache_list.Front()
			cache.cache_list.Remove(lru)
			delete(cache.cache_map,lru.Value.(KV).key)
		}


	} else{
		//Azurirace se vrednost pod zadatim kljucem - prethodna se brise
		cache.cache_list.Remove(found)
	}
	element := cache.cache_list.PushBack(KV{key:key,value: value})
	cache.cache_map[key] = element
}

//Kada se uputi delete zahtev, ako kljuca ima u cache - u, on se brise
//prima kljuc koji se brise
func (cache *Cache) DeleteKey(key string){
	value := cache.Search(key)
	if value != nil{
		cache.cache_list.Remove(value)
		delete(cache.cache_map,key)

	}else{
		println("Kljuc ne postoji u cache-u!")
	}
}


/*Testiranje - brise se u izvrsnoj verziji*/
/*func main(){
	cache := createCache(3)
	/*println(cache.Search("1").Value.(string))
	cache.printCache()
	cache.Insert("1",[]byte("maja"))
	cache.printCache()
	cache.Insert("2",[]byte("varga"))
	cache.printCache()
	cache.Insert("3",[]byte("overflow"))
	cache.printCache()
	println(string(cache.Search("2").Value.(KV).value))
	cache.printCache()
	cache.Insert("3",[]byte("another_overflow"))
	cache.printCache()
	cache.Insert("4",[]byte("too_much"))
	cache.printCache()
	println(cache.Search("1"))
	println(string(cache.Search("2").Value.(KV).value))
	cache.printCache()
	cache.DeleteKey("4")
	cache.printCache()
	cache.Search("3")
	cache.printCache()
}*/
