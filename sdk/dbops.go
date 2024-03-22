package sdk

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/avast/retry-go"
	"github.com/soumitsalman/beansack/store"
	datautils "github.com/soumitsalman/data-utils"
)

const (
	BEANSACK = "beansack"
	BEANS    = "beans"
	KEYWORDS = "keywords"
)

const (
	_MAX_TEXT_LENGTH    = 4096 * 4
	_MIN_KEYWORD_LENGTH = 3
	_NLP_OPS_BATCH_SIZE = 10
)

var (
	wholebeans   *store.Store[Bean]       = store.New(store.WithConnectionString[Bean](getConnectionString(), BEANSACK, BEANS))
	keywordstore *store.Store[KeywordMap] = store.New(store.WithConnectionString[KeywordMap](getConnectionString(), BEANSACK, KEYWORDS))
	nlpdriver    *ParrotBoxDriver         = NewParrotBoxDriver()
)

func AddBeans(beans []Bean) error {
	// remove items without a text body

	// assign updated time
	updated_time := time.Now().Unix()
	datautils.ForEach(beans, func(item *Bean) {
		item.Updated = updated_time
		item.Text = datautils.TruncateTextWithEllipsis(item.Text, _MAX_TEXT_LENGTH)
	})

	_, err := wholebeans.Add(beans)
	if err != nil {
		return err
	}
	// once the main docs are up, update them with sentiment, summary, keywords and embeddings
	updateNlpAttributes(beans)
	return nil
}

func GetBeans(filter store.JSON, fields store.JSON) []Bean {
	return wholebeans.Get(filter, fields)
}

func SimilaritySearch(query_text string, filter store.JSON, top_n int) []Bean {
	embeddings := runRemoteNlpFunction(nlpdriver.CreateTextEmbeddings, []string{query_text})[0].Embeddings
	return wholebeans.SimilaritySearch(embeddings, filter, top_n)
}

const (
	_FOUR_WEEKS = 28
	_ONE_DAY    = 1
)

func checkAndFixTimeWindow(time_window int) int {
	switch {
	case time_window > _FOUR_WEEKS:
		return _FOUR_WEEKS
	case time_window < _ONE_DAY:
		return _ONE_DAY
	default:
		return time_window
	}
}

// last_n_days can be between 1 - 30
func GetTrendingKeywords(time_window int) []KeywordMap {
	time_window = checkAndFixTimeWindow(time_window)
	trending_keys_pipeline := []store.JSON{
		{
			"$match": store.JSON{
				"updated": store.JSON{"$gte": timeValue(time_window)},
			},
		},
		{
			"$group": store.JSON{
				"_id":   "$keyword",
				"count": store.JSON{"$count": 1},
			},
		},
		{
			"$match": store.JSON{
				"count": store.JSON{"$gt": 2},
			},
		},
		{
			"$sort": store.JSON{"count": -1},
		},
		{
			"$project": store.JSON{
				"keyword": "$_id",
				"count":   1,
				"_id":     0,
			},
		},
	}
	return keywordstore.Aggregate(trending_keys_pipeline)
}

func GetTrendingBeans(time_window int) []Bean {
	time_window = checkAndFixTimeWindow(time_window)

	// first get the keywords with their counts
	trending_keywords := GetTrendingKeywords(time_window)

	// then get the URLs with has those keywords
	keywords := datautils.Transform(trending_keywords, func(item *KeywordMap) string { return item.Keyword })
	keyword_filter := store.JSON{
		"keyword": store.JSON{"$in": keywords},
	}
	keywordmaps := keywordstore.Get(keyword_filter, nil)
	log.Println(len(keywordmaps))

	// then get the Beans with those urls
	urls := datautils.Transform(keywordmaps, func(item *KeywordMap) string { return item.BeanUrl })
	// time_val := timeValue(last_n_days)
	latest_bean_filter := store.JSON{
		"url":     store.JSON{"$in": urls},
		"updated": store.JSON{"$gte": timeValue(time_window)},
	}
	fields := store.JSON{
		"text":       0,
		"embeddings": 0,
		"keywords":   0,
	}
	beans := GetBeans(latest_bean_filter, fields)
	return beans
}

func timeValue(last_n_days int) int64 {
	return time.Now().AddDate(0, 0, -last_n_days).Unix()
}

type NlpDriverError string

func (err NlpDriverError) Error() string {
	return string(err)
}

func updateNlpAttributes(beans []Bean) {
	filters := getPointerFilters(beans)
	texts := getTextFields(beans)

	// store embeddings
	embs := runRemoteNlpFunction(nlpdriver.CreateBeanEmbeddings, texts)
	wholebeans.Update(embs, filters)

	// store summary
	summaries := runRemoteNlpFunction(nlpdriver.CreateBeanSummary, texts)
	wholebeans.Update(summaries, filters)

	// store the keywords in combination with existing keywords
	keywords_list := runRemoteNlpFunction(nlpdriver.CreateBeanKeywords, texts)
	for i := range beans {
		keywordstore.Add(datautils.Transform(append(beans[i].Keywords, keywords_list[i].Keywords...), func(item *string) KeywordMap {
			return KeywordMap{
				Keyword: *item,
				BeanUrl: beans[i].Url,
				Updated: beans[i].Updated,
			}
		}))
	}
}

func runRemoteNlpFunction[T any](nlp_func func(texts []string) ([]T, error), texts []string) []T {
	res := make([]T, 0, len(texts))

	// try small batches
	for i := 0; i < len(texts); i += _NLP_OPS_BATCH_SIZE {
		input := datautils.SafeSlice(texts, i, i+_NLP_OPS_BATCH_SIZE)
		// retry for each batch
		retry.Do(func() error {
			output, err := nlp_func(input)
			// something went wrong with the function so try again
			if err != nil {
				return err
			} else if len(output) != len(input) {
				msg := fmt.Sprintf("[dbops] Remote NLP function failed. Output length %d does not match input length %d", len(output), len(input))
				log.Println(msg)
				return NlpDriverError(msg)
			}
			// generation succeeded
			res = append(res, output...)
			return nil
		}, retry.Delay(_RETRY_DELAY))
	}
	return res
}

func getPointerFilters(beans []Bean) []store.JSON {
	return datautils.Transform(beans, func(bean *Bean) store.JSON {
		return store.JSON{
			"url":     bean.Url,
			"updated": bean.Updated,
		}
	})
}

func getTextFields(beans []Bean) []string {
	return datautils.Transform(beans, func(bean *Bean) string {
		return bean.Text
	})
}

func getConnectionString() string {
	return os.Getenv("DB_CONNECTION_STRING")
}
