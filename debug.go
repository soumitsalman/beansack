package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/soumitsalman/beansack/sdk"
	"github.com/soumitsalman/beansack/store"
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
	// filter for retrieving items for the last 2 days
	recent_filter := store.JSON{
		"updated": store.JSON{
			"$gte": time.Now().AddDate(0, 0, -2).Unix(),
		},
	}

	res := sdk.SimilaritySearch("cyber attack", recent_filter, 20)
	fmt.Println(len(res))
	for _, v := range res {
		fmt.Println(v.BeanUrl)
	}
}
