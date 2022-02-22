package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"time"
)

var (
	tokensPerReset     uint32 = 60 // broj tokena koji se deli po resetu
	minutesBeforeReset uint32 = 1  // vremenski interval posle kojeg se desava reset i punjenje baketa
)

// vraca trenutno vreme u unix sekundama
func now() uint64 {
	return uint64(time.Now().Unix())
}

// proverava da li je vreme u argumentu iz proslosti
func isPast(stored uint64) bool {
	return stored < now()
}

// formira niz bajtova sa novim vrednostima
func formBytes(time uint64, tokens uint32) []byte {
	bytes := make([]byte, 12)
	binary.LittleEndian.PutUint64(bytes[:8], time)
	binary.LittleEndian.PutUint32(bytes[8:], tokens)
	return bytes
}

// formira niz bajtova sa inicijalnim vrednostima
func formInitialBytes() []byte {
	return formBytes(now(), tokensPerReset-1)
}

// LoadConfig - ucitava podesavanja iz eksterne datoteke
func LoadConfig() error {
	config, err := os.Open("config.txt")
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(config)

	scanner.Scan() // preskacemo linije nevezane za token bucket
	scanner.Scan()
	scanner.Scan()

	scanner.Scan()
	tokens, err := strconv.Atoi(scanner.Text()[7:])
	if err != nil {
		return err
	}
	tokensPerReset = uint32(tokens)

	scanner.Scan()
	minutes, err := strconv.Atoi(scanner.Text()[8:])
	if err != nil {
		return err
	}
	minutesBeforeReset = uint32(minutes)

	err = config.Close()
	if err != nil {
		return err
	}
	return nil
}

// InitializeTokenBucketConfigs - funkcija koja inicijalizuje konfiguracije potrebne za funkcionisanje token bucket-a
func InitializeTokenBucketConfigs(tokensPerReset_ uint32, minutesBeforeReset_ uint32) {
	tokensPerReset = tokensPerReset_
	minutesBeforeReset = minutesBeforeReset_
}

// CheckTokenBucket - funkcija koja implementira token bucket algoritam
func CheckTokenBucket(user string) bool {

	val := system.get("", user)

	if len(val) <= 0 { // ovaj korisnik prvi put pravi zahtev, dozvoli i puttuj inicijalne vrednosti za njega u mapu
		system.put("", user, formInitialBytes())
		return true
	} else { // korisnik je vec pravio zahteve
		timestamp := binary.LittleEndian.Uint64(val[:8])       // vreme proslog reseta
		if isPast(timestamp + uint64(minutesBeforeReset)*60) { // interval je prosao, punimo token bucket ponovo i resetujemo vreme
			system.put("", user, formInitialBytes())
			return true
		} else { // interval nije prosao
			tokens := binary.LittleEndian.Uint32(val[8:])
			if tokens >= 1 { // jos ima tokena, oduzimamo 1 token i dozvoljavamo
				system.put("", user, formBytes(timestamp, tokens-1))
				return true
			} else { // nema vise tokena, zahtev odbijen
				fmt.Println("Previse zahteva u ovom periodu vremena, zahtev odbijen. Molim Vas sacekajte.")
				return false
			}
		}
	}
}
