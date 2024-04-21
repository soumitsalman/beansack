package sdk

import (
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/avast/retry-go"
	"github.com/soumitsalman/beansack/store"
	datautils "github.com/soumitsalman/data-utils"
)

const (
	BEANSACK = "beansack"
	BEANS    = "beans"
	NOISES   = "noises"
	KEYWORDS = "keywords"
)

const (
	_MIN_TEXT_LENGTH    = 20
	_MAX_TEXT_LENGTH    = 4096 * 4
	_MIN_KEYWORD_LENGTH = 3
	_NLP_OPS_BATCH_SIZE = 10
)

const (
	_FOUR_WEEKS = 28
	_ONE_DAY    = 1
)

var (
	wholebeans   *store.Store[Bean]
	keywordstore *store.Store[KeywordMap]
	noises       *store.Store[BeanMediaNoise]
	remote_nlp   *ParrotBoxDriver
	local_nlp    *InferenceDriver
	nlpqueue     chan []Bean
)

var (
	bean_fields = store.JSON{
		"url":       1,
		"updated":   1,
		"source":    1,
		"title":     1,
		"kind":      1,
		"author":    1,
		"created":   1,
		"summary":   1,
		"keywords":  1,
		"topic":     1,
		"sentiment": 1,
	}
	generated_fields = []string{"category_embeddings", "search_embeddings" /*"topic", "keywords", "summary", "sentiment" */}
)

type BeanSackError string

func (err BeanSackError) Error() string {
	return string(err)
}

func InitializeBeanSack(db_conn_str, parrotbox_url, parrotbox_auth_token string) error {
	wholebeans = store.New(
		store.WithConnectionString[Bean](db_conn_str, BEANSACK, BEANS),
		store.WithMinSearchScore[Bean](0.55), // TODO: change this to 0.8 in future
		store.WithSearchTopN[Bean](5),
		store.WithDataIDAndEqualsFunction(getBeanId, Equals),
	)
	noises = store.New(
		store.WithConnectionString[BeanMediaNoise](db_conn_str, BEANSACK, NOISES),
	)
	keywordstore = store.New(store.WithConnectionString[KeywordMap](db_conn_str, BEANSACK, KEYWORDS))
	if wholebeans == nil || keywordstore == nil {
		return BeanSackError("Initialization Failed. db_conn_str Not working: " + db_conn_str)
	}
	remote_nlp = NewParrotBoxDriver(parrotbox_url, parrotbox_auth_token)
	local_nlp = NewLocalInferenceDriver()
	nlpqueue = make(chan []Bean, 100)
	go processNlpQueue()

	return nil
}

func AddBeans(beans []Bean) error {
	// remove items without a text body
	beans = datautils.Filter(beans, func(item *Bean) bool { return len(item.Text) > _MIN_TEXT_LENGTH })

	// extract out the beans medianoises
	medianoises := datautils.FilterAndTransform(beans, func(item *Bean) (bool, BeanMediaNoise) {
		if item.MediaNoise != nil {
			item.MediaNoise.BeanUrl = item.Url
			return true, *item.MediaNoise
		} else {
			return false, BeanMediaNoise{}
		}
	})

	// assign updated time and truncate text
	updated_time := time.Now().Unix()
	datautils.ForEach(beans, func(item *Bean) {
		item.Updated = updated_time
		item.Text = datautils.TruncateTextWithEllipsis(item.Text, _MAX_TEXT_LENGTH)
		item.MediaNoise = nil
	})
	datautils.ForEach(medianoises, func(item *BeanMediaNoise) {
		item.Updated = updated_time
		item.Digest = datautils.TruncateTextWithEllipsis(item.Digest, _MAX_TEXT_LENGTH)
	})

	beans, err := wholebeans.Add(beans)
	if err != nil {
		return err
	}
	noises.Add(medianoises)
	// once the main docs are up, update them with sentiment, summary, keywords and embeddings
	beans = datautils.Filter(beans, func(item *Bean) bool { return item.Kind != CHANNEL })
	go queueForNlp(beans)
	return nil
}

func GetBeans(filter_options ...Option) []Bean {
	return wholebeans.Get(makeFilter(filter_options...), bean_fields)
}

func TextSearchBeans(query_texts []string, filter_options ...Option) []Bean {
	filter := makeFilter(filter_options...)
	return wholebeans.TextSearch(query_texts, filter, bean_fields)
}

// func SimilaritySearchBeans(search_context string, filter_options ...Option) []Bean {
// 	log.Println("[dbops] Generating embeddings for:", search_context)
// 	search_vector := runRemoteNlpFunction(nlpdriver.CreateTextEmbeddings, []string{search_context})[0]
// 	filter := makeFilter(filter_options...)
// 	return wholebeans.VectorSearch(search_vector.Embeddings, filter, bean_fields)
// }

func CategorySearchBeans(categories []string, filter_options ...Option) []Bean {
	filter := makeFilter(filter_options...)

	log.Println("[dbops] Generating embeddings for:", categories)
	search_vectors := createBatchEmbeddings(categories, CATEGORIZATION)
	result := make([]Bean, 0, len(categories)*5)
	datautils.ForEach(search_vectors, func(vec *[]float32) {
		result = append(result, wholebeans.VectorSearch(*vec, "category_embeddings", filter, bean_fields)...)
	})
	return result
}

func QuerySearchBeans(search_query string, filter_options ...Option) []Bean {
	filter := makeFilter(filter_options...)

	log.Println("[dbops] Generating embeddings for:", search_query)
	search_vector := createEmbeddings(search_query, SEARCH_QUERY)
	return wholebeans.VectorSearch(search_vector, "search_embeddings", filter, bean_fields)
}

func todo_GetMediaNoise(beans []Bean) []Bean {
	// urls := datautils.Transform(beans, func(item *Bean) string { return item.Url })
	// noise_items := noises.Get(
	// 	store.JSON{
	// 		"url": store.JSON{"$in": urls},
	// 	},
	// 	store.JSON{"_id": 0},
	// )
	return beans
}

// this is for any recurring service
// this is currently not being run as a recurring service
func RectifyBeans() {
	// delete old stuff
	wholebeans.Delete(
		store.JSON{
			"updated": store.JSON{"$lte": timeValue(checkAndFixTimeWindow(15))},
		},
	)
	keywordstore.Delete(
		store.JSON{
			"updated": store.JSON{"$lte": timeValue(checkAndFixTimeWindow(15))},
		},
	)

	for _, field_name := range generated_fields {
		beans := wholebeans.Get(
			store.JSON{
				field_name: store.JSON{"$exists": false},
				"updated":  store.JSON{"$gte": timeValue(checkAndFixTimeWindow(2))},
			},
			store.JSON{
				"url":  1,
				"text": 1,
			},
		)
		log.Printf("[dbops] Rectifying %s for %d items\n", field_name, len(beans))

		// process batch
		filters := getBeanIdFilters(beans)
		texts := getTextFields(beans)
		var updates []Bean

		// store embeddings
		// embs := runRemoteNlpFunction(nlpdriver.CreateBeanEmbeddings, texts)
		switch field_name {
		case "category_embeddings":
			updates = datautils.Transform(texts, func(item *string) Bean {
				return Bean{CategoryEmbeddings: createEmbeddings(*item, CATEGORIZATION)}
			})

		case "search_embeddings":
			updates = datautils.Transform(texts, func(item *string) Bean {
				return Bean{SearchEmbeddings: createEmbeddings(*item, SEARCH_DOCUMENT)}
			})
		}
		wholebeans.Update(updates, filters)
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

func queueForNlp(beans []Bean) {
	// break them in chunks
	for batch_i := 0; batch_i < len(beans); batch_i += _NLP_OPS_BATCH_SIZE {
		batch_beans := datautils.SafeSlice(beans, batch_i, batch_i+_NLP_OPS_BATCH_SIZE)
		nlpqueue <- batch_beans
	}
}

func processNlpQueue() {
	for {
		beans := <-nlpqueue

		log.Println("NLP processing a batch of", len(beans), "items")
		// process batch
		filters := getBeanIdFilters(beans)
		texts := getTextFields(beans)
		var updates []Bean

		// store embeddings for Category
		updates = datautils.Transform(texts, func(item *string) Bean {
			return Bean{CategoryEmbeddings: createEmbeddings(*item, CATEGORIZATION)}
		})
		wholebeans.Update(updates, filters)

		// store embeddings for CHAT search
		updates = datautils.Transform(texts, func(item *string) Bean {
			return Bean{SearchEmbeddings: createEmbeddings(*item, SEARCH_DOCUMENT)}
		})
		wholebeans.Update(updates, filters)

		// store summary
		summaries := runRemoteNlpFunction(remote_nlp.CreateBeanSummary, texts)
		wholebeans.Update(summaries, filters)

		// store the keywords in combination with existing keywords
		// both in keywords collection
		// and the beans collection for easy retrieval
		keywords_list := runRemoteNlpFunction(remote_nlp.CreateBeanKeywords, texts)
		for i := range beans {
			keywords_list[i].Keywords = append(beans[i].Keywords, keywords_list[i].Keywords...)
			keywordstore.Add(datautils.Transform(keywords_list[i].Keywords, func(item *string) KeywordMap {
				*item = strings.ToLower(*item)
				return KeywordMap{
					Keyword: strings.ToLower(*item),
					BeanUrl: beans[i].Url,
					Updated: beans[i].Updated,
				}
			}))
		}
		wholebeans.Update(keywords_list, filters)
	}
}

func runRemoteNlpFunction[T any](nlp_func func(texts []string) ([]T, error), texts []string) []T {
	var res []T
	// retry for each batch
	retry.Do(func() error {
		output, err := nlp_func(texts)
		// something went wrong with the function so try again
		if err != nil {
			return err
		} else if len(output) != len(texts) {
			msg := fmt.Sprintf("[dbops] Remote NLP function failed. Output length %d does not match input length %d", len(output), len(texts))
			log.Println(msg)
			return BeanSackError(msg)
		}
		// generation succeeded
		res = output
		return nil
	}, retry.Delay(_RETRY_DELAY))

	return res
}

func createEmbeddings(text string, task_type string) []float32 {
	return retryWhenFails[string, []float32](
		func(input string) ([]float32, error) {
			return local_nlp.CreateTextEmbeddings(text, task_type)
		},
		text)
}

func createBatchEmbeddings(texts []string, task_type string) [][]float32 {
	return retryWhenFails(
		func(input []string) ([][]float32, error) {
			return local_nlp.CreateBatchTextEmbeddings(texts, task_type)
		},
		texts)
}

func retryWhenFails[T_input, T_output any](original_func func(input T_input) (T_output, error), input T_input) T_output {
	var res T_output
	var err error
	// retry for each batch
	retry.Do(func() error {
		res, err = original_func(input)
		// something went wrong with the function so try again
		if err != nil {
			return err
		}
		return nil
	}, retry.Delay(_RETRY_DELAY))
	return res
}

func getBeanId(bean *Bean) store.JSON {
	return store.JSON{"url": bean.Url}
}

func getBeanIdFilters(beans []Bean) []store.JSON {
	return datautils.Transform(beans, func(bean *Bean) store.JSON {
		return getBeanId(bean)
	})
}

func getTextFields(beans []Bean) []string {
	return datautils.Transform(beans, func(bean *Bean) string {
		return bean.Text
	})
}

func makeFilter(filter_options ...Option) store.JSON {
	filter := store.JSON{}
	for _, opt := range filter_options {
		opt(filter)
	}
	return filter
}

func timeValue(time_window int) int64 {
	return time.Now().AddDate(0, 0, -time_window).Unix()
}

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
