package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/soumitsalman/beansack/sdk"
)

func loadFromFile(filepath string) []sdk.Bean {
	file, _ := os.Open(filepath)
	var output []sdk.Bean
	json.NewDecoder(file).Decode(&output)
	return output
}

func main() {
	// path := "/home/soumitsr/Codes/newscollector/2024-03-18-14-55-11.json"
	// beans := loadFromFile(path)
	// sdk.AddBeans(beans)

	// fmt.Println(len(sdk.GetBeans(store.JSON{"kind": sdk.ARTICLE})))
	res := sdk.SimilaritySearch("cyber attack")
	fmt.Println(len(res))
	for _, v := range res {
		fmt.Println(len(v.Text))
	}
}
