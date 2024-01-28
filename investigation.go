package vfs

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func Bench(configPath string) {
	cfg, err := os.ReadFile(configPath)
	if err != nil {
		fmt.Println(err)
		return
	}
	var c IndexConfig
	if err = json.Unmarshal(cfg, &c); err != nil {
		fmt.Println(err)
		return
	}

	index, err := NewIndex(&c)
	if err != nil {
		fmt.Println(err)
		return
	}

	start := time.Now()
	next := time.Now().Add(time.Second)
	count := 0
	for {
		txn, err := index.BeginTransaction()
		if err != nil {
			panic(err)
		}

		key := make([]byte, 20)
		rand.Read(key)

		if err = txn.Set(key, 0, uint64(count)); err != nil {
			panic(err)
		}

		err = txn.EndTransaction()
		if err != nil {
			panic(err)
		}

		count++

		if time.Now().After(next) {
			delta := time.Since(start)
			fmt.Printf("%d  %d/s\n", count, count/int(delta.Seconds()))
			next = time.Now().Add(time.Second)
		}
	}
}
