package goignite_test

import (
	"fmt"
	"github.com/RealRickyUser/goignite"
)

func ExampleIgniteClient_GetOrCreateCache() {
	client := goignite.NewClient("127.0.0.1:10800")
	err := client.Connect()
	if err != nil {
		panic(err)
	}
	defer client.Close()
	err = client.GetOrCreateCache("new_cache_name")
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
