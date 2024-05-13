package sdk

import (
	"log"
	"time"

	"github.com/soumitsalman/beansack/nlp/embeddings"
	"github.com/soumitsalman/beansack/nlp/parrotbox"
	"github.com/soumitsalman/beansack/store"
	datautils "github.com/soumitsalman/data-utils"
)

const (
	BEANSACK = "beansack"
	BEANS    = "beans"
	NOISES   = "noises"
	KEYWORDS = "keywords"
	CONCEPTS = "concepts"
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
	beanstore    *store.Store[Bean]
	keywordstore *store.Store[KeywordMap]
	conceptstore *store.Store[BeanConcept]
	noises       *store.Store[BeanMediaNoise]
	// remote_nlp   *nlp.PyParrotBoxDriver
	emb_client *embeddings.EmbeddingsDriver
	pb_client  *parrotbox.GoParrotboxClient

	// nlpqueue chan []Bean
)

var (
	fields = store.JSON{
		// for beans
		"url":      1,
		"updated":  1,
		"source":   1,
		"title":    1,
		"kind":     1,
		"author":   1,
		"created":  1,
		"summary":  1,
		"keywords": 1,
		"topic":    1,

		// for media noise
		"score":     1,
		"sentiment": 1,

		// for concepts
		"keyphrase":   1,
		"description": 1,
	}
	generated_fields = []string{"category_embeddings", "search_embeddings", "summary" /* "sentiment" */}
)

type BeanSackError string

func (err BeanSackError) Error() string {
	return string(err)
}

func InitializeBeanSack(db_conn_str, parrotbox_auth_token string) error {
	beanstore = store.New(db_conn_str, BEANSACK, BEANS,
		store.WithMinSearchScore[Bean](0.55), // TODO: change this to 0.8 in future
		store.WithSearchTopN[Bean](5),
		store.WithDataIDAndEqualsFunction(getBeanId, Equals),
	)
	noises = store.New[BeanMediaNoise](db_conn_str, BEANSACK, NOISES)
	keywordstore = store.New[KeywordMap](db_conn_str, BEANSACK, KEYWORDS)
	conceptstore = store.New[BeanConcept](db_conn_str, BEANSACK, CONCEPTS)

	if beanstore == nil || keywordstore == nil || conceptstore == nil {
		return BeanSackError("Initialization Failed. db_conn_str Not working: " + db_conn_str)
	}

	pb_client = parrotbox.NewGoParrotboxClient(parrotbox_auth_token)
	emb_client = embeddings.NewEmbeddingsDriver()

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
	update_time := time.Now().Unix()
	datautils.ForEach(beans, func(item *Bean) {
		item.ID = item.Url
		item.Updated = update_time
		item.Text = datautils.TruncateTextWithEllipsis(item.Text, _MAX_TEXT_LENGTH)
		item.MediaNoise = nil
	})
	datautils.ForEach(medianoises, func(item *BeanMediaNoise) {
		item.Updated = update_time
		item.Digest = datautils.TruncateTextWithEllipsis(item.Digest, _MAX_TEXT_LENGTH)
	})

	// now store the beans before processing them for generated fields
	beans, err := beanstore.Add(beans)
	if err != nil {
		log.Println(err)
		return err
	}
	// now store the medianoises. But no need to check for error since their storage is auxiliary for the overall experience
	noises.Add(medianoises)

	// once the main docs are up, update them with topic, summary, keyconcepts and embeddings
	beans = datautils.Filter(beans, func(item *Bean) bool { return item.Kind != CHANNEL })
	go generateCustomFields(beans, update_time)
	return nil
}

func GetBeans(filter_options ...Option) []Bean {
	return beanstore.Get(makeFilter(filter_options...), fields)
}

func TextSearchBeans(query_texts []string, filter_options ...Option) []Bean {
	filter := makeFilter(filter_options...)
	return beanstore.TextSearch(query_texts, filter, fields)
}

func CategorySearchBeans(categories []string, filter_options ...Option) []Bean {
	filter := makeFilter(filter_options...)

	log.Println("[dbops] Generating embeddings for:", categories)
	search_vectors := emb_client.CreateBatchTextEmbeddings(categories, embeddings.CATEGORIZATION)
	result := make([]Bean, 0, len(categories)*5)
	datautils.ForEach(search_vectors, func(vec *[]float32) {
		result = append(result, beanstore.VectorSearch(*vec, "category_embeddings", filter, fields)...)
	})
	return result
}

func QuerySearchBeans(search_query string, filter_options ...Option) []Bean {
	filter := makeFilter(filter_options...)

	log.Println("[dbops] Generating embeddings for:", search_query)
	search_vector := emb_client.CreateTextEmbeddings(search_query, embeddings.SEARCH_QUERY)
	return beanstore.VectorSearch(search_vector, "search_embeddings", filter, fields)
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
	beanstore.Delete(
		store.JSON{
			"updated": store.JSON{"$lte": timeValue(checkAndFixTimeWindow(15))},
		},
	)
	keywordstore.Delete(
		store.JSON{
			"updated": store.JSON{"$lte": timeValue(checkAndFixTimeWindow(15))},
		},
	)

	// generate the fields that do not exist
	for _, field_name := range generated_fields {
		beans := beanstore.Get(
			store.JSON{
				field_name: store.JSON{"$exists": false},
				// "updated":  store.JSON{"$gte": timeValue(checkAndFixTimeWindow(2))},
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
		// store embeddings
		updateBeans(field_name, texts, filters)
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

func generateCustomFields(beans []Bean, batch_update_time int64) {
	log.Println("Generating fields for a batch of", len(beans), "items")

	// process batch
	filters := getBeanIdFilters(beans)
	texts := getTextFields(beans)

	// update beans with the generated fields
	datautils.ForEach(generated_fields, func(field *string) { updateBeans(*field, texts, filters) })

	// extract key concepts
	concepts := datautils.Transform(pb_client.ExtractKeyConcepts(texts), NewKeyConcept)
	// generate embeddings for the descriptions so that we can match it with the documents
	concept_descriptions := datautils.Transform(concepts, func(item *BeanConcept) string { return item.Description })
	embs := emb_client.CreateBatchTextEmbeddings(concept_descriptions, embeddings.CATEGORIZATION)
	// it is possible that embeddings generation failed even after retry.
	// if things failed no need to insert those items
	if len(embs) == len(concepts) {

		for i := 0; i < len(concepts); i++ {
			concepts[i].Embeddings = embs[i]
			concepts[i].Updated = batch_update_time
		}
		conceptstore.Add(concepts)
	}
}

func updateBeans(field_name string, texts []string, filters []store.JSON) {
	var updates []any

	switch field_name {
	case "category_embeddings":
		updates = datautils.Transform(texts, func(item *string) any {
			return Bean{CategoryEmbeddings: emb_client.CreateTextEmbeddings(*item, embeddings.CATEGORIZATION)}
		})

	case "search_embeddings":
		updates = datautils.Transform(texts, func(item *string) any {
			return Bean{SearchEmbeddings: emb_client.CreateTextEmbeddings(*item, embeddings.SEARCH_DOCUMENT)}
		})

	case "summary":
		updates = datautils.Transform(pb_client.ExtractDigests(texts), func(item *parrotbox.Digest) any { return item })
	}

	beanstore.Update(updates, filters)
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
