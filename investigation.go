package vfs

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	mrand "math/rand"
	"os"
	"time"
)

func Bench(configPath string) {
	cfg, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	var c VfsConfig
	if err = json.Unmarshal(cfg, &c); err != nil {
		fmt.Println(err)
		return
	}

	st, err := NewStore(&c)
	if err != nil {
		fmt.Println(err)
		return
	}

	start := time.Now()
	next := time.Now().Add(time.Second)
	count := 0
	for {
		records := make([]StoreRecord, 0, 128)
		for i := 0; i < 128; i++ {
			key := make([]byte, 20)
			rand.Read(key)
			datalen := mrand.Intn(16384) + 1
			data := make([]byte, datalen)
			rand.Read(data)

			records = append(records, StoreRecord{key, data})
			count++
		}

		if err = st.StoreContent(records); err != nil {
			panic(err)
		}

		if time.Now().After(next) {
			delta := time.Since(start)
			fmt.Printf("%d  %d/s\n", count, count/int(delta.Seconds()))
			next = time.Now().Add(time.Second)

			if err = st.PurgeOld(); err != nil {
				panic(err)
			}
		}
	}
}
