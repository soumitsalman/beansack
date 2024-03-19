package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/soumitsalman/beansack/sdk"
	"github.com/soumitsalman/beansack/store"
	datautils "github.com/soumitsalman/data-utils"
)

func loadFromFile(filepath string) []sdk.Bean {
	file, _ := os.Open(filepath)
	var output []sdk.Bean
	json.NewDecoder(file).Decode(&output)
	return output
}

func main() {
	path := "/home/soumitsr/Codes/newscollector/2024-03-18-14-55-11.json"
	datautils.ForEach(sdk.AddDigest(loadFromFile(path)), func(item *string) { fmt.Println(*item) })
	fmt.Println(len(sdk.GetBeans(store.JSON{"kind": sdk.ARTICLE})))
}
