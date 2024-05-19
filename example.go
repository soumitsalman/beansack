package main

import (
	"log"
	"os"
	"time"

	"github.com/joho/godotenv"
	"github.com/soumitsalman/beansack/sdk"
	datautils "github.com/soumitsalman/data-utils"
)

func main() {
	godotenv.Load()
	// initialize the services
	if err := sdk.InitializeBeanSack(os.Getenv("DB_CONNECTION_STRING"), os.Getenv("LLMSERVICE_API_KEY")); err != nil {
		log.Fatalln("initialization not working", err)
	}

	// search and query
	var beans []sdk.Bean
	// test vector search
	beans = sdk.FuzzySearchBeans(&sdk.SearchOptions{CategoryTexts: []string{"Russia's longest-serving minister, has been removed as defence minister by President Vladimir Putin"}})
	datautils.ForEach(beans, func(item *sdk.Bean) { log.Printf("%f | %s\n", item.SearchScore, item.Title) })

	// test text search
	beans = sdk.TextSearch([]string{"Sergei Shoigu", "Being removed as defence minister"}, sdk.NewSearchOptions())
	datautils.ForEach(beans, func(item *sdk.Bean) { log.Printf("%f | %s\n", item.SearchScore, item.Title) })

	// nugget search
	beans = sdk.NuggetSearch([]string{"Cinterion cellular modems"}, sdk.NewSearchOptions().WithTimeWindow(2))
	datautils.ForEach(beans, func(item *sdk.Bean) {
		log.Printf("[%s] %s | %s\n", item.Source, time.Unix(item.Updated, 0).Format(time.DateTime), item.Title)
	})

	// trending nuggets
	nuggets := sdk.TrendingNuggets(sdk.NewSearchOptions().WithTimeWindow(2))
	datautils.ForEach(nuggets, func(item *sdk.NewsNugget) { log.Printf("%d | %s: %s\n", item.TrendScore, item.KeyPhrase, item.Event) })

}
