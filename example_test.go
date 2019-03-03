package goignite_test

import (
	"fmt"
	"github.com/laminy/goignite"
)

func ExampleIgniteClient_GetOrCreateCache() {
	client := goignite.NewClient("127.0.0.1:10800")
	err := client.Connect()
	if err != nil {
		panic(err)
	}
	defer client.Close()
	_, err = client.GetOrCreateCache("new_cache_name")
	if err != nil {
		fmt.Println(err)
	}
}

func ExampleIgniteClient_Connect() {
	client := goignite.NewClient("127.0.0.1:10800")
	err := client.Connect()
	if err != nil {
		panic(err)
	}
	defer client.Close()
}

func ExampleIgniteClient_GetCacheNames() {
	client := goignite.NewClient("127.0.0.1:10800")
	err := client.Connect()
	if err != nil {
		panic(err)
	}
	defer client.Close()
	caches, err := client.GetCacheNames()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(caches)
}

func ExampleIgniteCache_Put() {
	client := goignite.NewClient("127.0.0.1:10800")
	err := client.Connect()
	if err != nil {
		panic(err)
	}
	defer client.Close()

	cache, err := client.GetOrCreateCache("myCacheName")
	if err != nil {
		fmt.Println(err)
	}

	key := byte(1)
	value := "sampleValue"

	err = cache.Put(key, value)
	if err != nil {
		fmt.Println(err)
	}
}

func ExampleIgniteCache_Get() {
	client := goignite.NewClient("127.0.0.1:10800")
	err := client.Connect()
	if err != nil {
		panic(err)
	}
	defer client.Close()

	cache, err := client.GetOrCreateCache("myCacheName")
	if err != nil {
		fmt.Println(err)
	}

	key := byte(1)
	value := "sampleValue"

	err = cache.Put(key, value)
	if err != nil {
		fmt.Println(err)
	}
	var actual string
	err = cache.Get(key, &actual)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Actual value from cache = %s\n", actual)
}
