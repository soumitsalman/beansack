package main

import (
	"encoding/json"
	"fmt"
	"os"

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
	path := "/home/soumitsr/Codes/newscollector/2024-03-18-14-55-11.json"
	beans := loadFromFile(path)
	// datautils.ForEach(sdk.CreateAttributes(beans), func(item *sdk.Bean) { fmt.Println(item.Url, item.Sentiment) })
	// datautils.ForEach(sdk.CreateBeanEmbeddings(beans), func(item *sdk.BeanEmbeddings) { fmt.Println(item.BeanUrl, len(item.Embeddings)) })
	sdk.AddBeans(beans)

	fmt.Println(len(sdk.GetBeans(store.JSON{"kind": sdk.ARTICLE})))
}
