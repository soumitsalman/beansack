package sdk

import (
	"fmt"
	"log"
	"os"
	"strings"
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

const (
	_FOUR_WEEKS = 28
	_ONE_DAY    = 1
)

var (
	wholebeans *store.Store[Bean] = store.New(
		store.WithConnectionString[Bean](getConnectionString(), BEANSACK, BEANS),
		store.WithMinSearchScore[Bean](0.0), // TODO: change this to 0.8 in future
		store.WithSearchTopN[Bean](15),
	)
	// wholebeans_V2 *store.Store[map[string]any] = store.New(store.WithConnectionString[map[string]any](getConnectionString(), BEANSACK, BEANS))
	keywordstore *store.Store[KeywordMap] = store.New(store.WithConnectionString[KeywordMap](getConnectionString(), BEANSACK, KEYWORDS))
	nlpdriver    *ParrotBoxDriver         = NewParrotBoxDriver()
)

var (
	bean_fields = store.JSON{
		"url":       1,
		"updated":   1,
		"source":    1,
		"title":     1,
		"kind":      1,
		"author":    1,
		"published": 1,
		"summary":   1,
		"keywords":  1,
	}
)

type Option func(filter store.JSON)

func WithKeywordsFilter(keywords []string) Option {
	return func(filter store.JSON) {
		filter["keywords"] = store.JSON{"$in": keywords}
	}
}

func WithTrendingFilter(time_window int) Option {
	return func(filter store.JSON) {
		keywords := datautils.Transform(GetTrendingKeywords(time_window), func(item *KeywordMap) string { return item.Keyword })
		filter["keywords"] = store.JSON{"$in": keywords}
		filter["updated"] = store.JSON{"$gte": timeValue(checkAndFixTimeWindow(time_window))}
	}
}

func WithTimeWindowFilter(time_window int) Option {
	return func(filter store.JSON) {
		filter["updated"] = store.JSON{"$gte": timeValue(checkAndFixTimeWindow(time_window))}
	}
}

func WithKindFilter(kind string) Option {
	return func(filter store.JSON) {
		filter["kind"] = kind
	}
}

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
	updateBeansWithAttributes(beans)
	return nil
}

func SearchBeans(query_texts []string, query_embeddings [][]float32, filter_options ...Option) []Bean {
	// if query embeddings is nil or empty then make it up from query_text
	if len(query_embeddings) == 0 {
		log.Println("[dbops] Generating embeddings for:", query_texts)
		query_embeddings = datautils.Transform(runRemoteNlpFunction(nlpdriver.CreateTextEmbeddings, query_texts), func(item *TextEmbeddings) []float32 { return item.Embeddings })
	}
	filter := makeFilter(filter_options...)
	beans := make([]Bean, 0, 3*len(query_embeddings)) // approximate length
	for _, emb := range query_embeddings {
		beans = append(beans, wholebeans.Search(emb, filter, bean_fields)...)
	}

	return beans
}

func GetBeans(filter_options ...Option) []Bean {
	return wholebeans.Get(makeFilter(filter_options...), bean_fields)
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

type NlpDriverError string

func (err NlpDriverError) Error() string {
	return string(err)
}

func updateBeansWithAttributes(beans []Bean) {
	filters := getPointerFilters(beans)
	texts := getTextFields(beans)

	// store embeddings
	embs := runRemoteNlpFunction(nlpdriver.CreateBeanEmbeddings, texts)
	wholebeans.Update(embs, filters)

	// store summary
	summaries := runRemoteNlpFunction(nlpdriver.CreateBeanSummary, texts)
	wholebeans.Update(summaries, filters)

	// store the keywords in combination with existing keywords
	// both in keywords collection
	// and the beans collection for easy retrieval
	keywords_list := runRemoteNlpFunction(nlpdriver.CreateBeanKeywords, texts)
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
