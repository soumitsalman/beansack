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
	BEANSACK    = "beansack"
	BEANS       = "beans"
	NOISES      = "noises"
	KEYWORDS    = "keywords"
	NEWSNUGGETS = "concepts"
)

// default configurations
const (
	// content length for processing for NLP driver
	_MIN_TEXT_LENGTH = 20

	// these 3 can be cut off
	_MAX_TEXT_LENGTH    = 4096 * 4
	_MIN_KEYWORD_LENGTH = 3
	_NLP_OPS_BATCH_SIZE = 10

	// time windows
	_FOUR_WEEKS = 28
	_ONE_DAY    = 1

	// vector and text search filters
	_TOPN_SEARCH      = 10
	_TOPN_NUGGET_MAP  = 100
	_MIN_SCORE_VECTOR = 0.67
	_MIN_SCORE_TEXT   = 10
)

var (
	beanstore       *store.Store[Bean]
	keywordstore    *store.Store[KeywordMap]
	newsnuggetstore *store.Store[NewsNugget]
	noisestore      *store.Store[MediaNoise]
	emb_client      *embeddings.EmbeddingsDriver
	pb_client       *parrotbox.GoParrotboxClient
)

const (
	_SEARCH_EMB   = "search_embeddings"
	_CATEGORY_EMB = "category_embeddings"
	_SUMMARY      = "summary"
)

var (
	_PROJECTION_FIELDS = store.JSON{
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
	_GENERATED_FIELDS = []string{_CATEGORY_EMB, _SEARCH_EMB, _SUMMARY}
)

type BeanSackError string

func (err BeanSackError) Error() string {
	return string(err)
}

func InitializeBeanSack(db_conn_str, parrotbox_auth_token string) error {
	beanstore = store.New(db_conn_str, BEANSACK, BEANS,
		// store.WithMinSearchScore[Bean](0.55), // TODO: change this to 0.8 in future
		// store.WithSearchTopN[Bean](10),
		store.WithDataIDAndEqualsFunction(getBeanId, Equals),
	)
	noisestore = store.New[MediaNoise](db_conn_str, BEANSACK, NOISES)
	keywordstore = store.New[KeywordMap](db_conn_str, BEANSACK, KEYWORDS)
	newsnuggetstore = store.New[NewsNugget](db_conn_str, BEANSACK, NEWSNUGGETS)

	if beanstore == nil || keywordstore == nil || newsnuggetstore == nil {
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
	medianoises := datautils.FilterAndTransform(beans, func(item *Bean) (bool, MediaNoise) {
		if item.MediaNoise != nil {
			item.MediaNoise.BeanUrl = item.Url
			return true, *item.MediaNoise
		} else {
			return false, MediaNoise{}
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
	datautils.ForEach(medianoises, func(item *MediaNoise) {
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
	noisestore.Add(medianoises)

	// once the main docs are up, update them with topic, summary, keyconcepts and embeddings
	beans = datautils.Filter(beans, func(item *Bean) bool { return item.Kind != CHANNEL })
	go generateCustomFields(beans, update_time)
	return nil
}

func GetBeans(filter_options ...Option) []Bean {
	return beanstore.Get(makeFilter(filter_options...), _PROJECTION_FIELDS)
}

func TextSearch(keywords []string, filter_options ...Option) []Bean {
	filter := makeFilter(filter_options...)
	return beanstore.TextSearch(keywords,
		store.WithTextFilter(filter),
		store.WithProjection(_PROJECTION_FIELDS),
		store.WithTextTopN(_TOPN_SEARCH),
		store.WithMinSearchScore(_MIN_SCORE_TEXT))
}

func NuggetSearch(nuggets []string, filter_options ...Option) []Bean {
	// get all the mapped urls
	initial_list := newsnuggetstore.Get(
		store.JSON{
			"keyphrase": store.JSON{"$in": nuggets},
		},
		store.JSON{"mapped_urls": 1},
	)
	// merge mapped_urls into one array
	mapped_urls := make([]string, 0, len(initial_list)*5)
	datautils.ForEach(initial_list, func(item *NewsNugget) { mapped_urls = append(mapped_urls, item.BeanUrls...) })

	// find the news articles with the urls in scope
	filter := makeFilter(filter_options...)
	filter["url"] = store.JSON{"$in": mapped_urls}
	return beanstore.Get(makeFilter(filter_options...), _PROJECTION_FIELDS)
}

func CategorySearch(categories []string, filter_options ...Option) []Bean {
	filter := makeFilter(filter_options...)

	log.Printf("[dbops] Generating embeddings for %d categories.\n", len(categories))
	search_vectors := emb_client.CreateBatchTextEmbeddings(categories, embeddings.CATEGORIZATION)
	result := make([]Bean, 0, len(categories)*5)
	datautils.ForEach(search_vectors, func(vec *[]float32) {
		result = append(result,
			beanstore.VectorSearch(*vec, _CATEGORY_EMB,
				store.WithVectorFilter(filter),
				store.WithProjection(_PROJECTION_FIELDS),
				store.WithMinSearchScore(_MIN_SCORE_VECTOR),
				store.WithVectorTopN(_TOPN_SEARCH))...)
	})
	return result
}

func ConversationContextSearch(search_query string, filter_options ...Option) []Bean {
	filter := makeFilter(filter_options...)

	log.Println("[dbops] Generating embeddings for:", search_query)
	search_vector := emb_client.CreateTextEmbeddings(search_query, embeddings.SEARCH_QUERY)
	return beanstore.VectorSearch(search_vector, _SEARCH_EMB,
		store.WithVectorFilter(filter),
		store.WithProjection(_PROJECTION_FIELDS),
		store.WithMinSearchScore(_MIN_SCORE_VECTOR),
		store.WithVectorTopN(_TOPN_SEARCH))
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
	for _, field_name := range _GENERATED_FIELDS {
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

func remapNewsNuggets() {
	nuggets := newsnuggetstore.Get(
		store.JSON{
			"embeddings": store.JSON{"$exists": true}, // ignore if a nugget if it doesnt have an embedding
		},
		nil)

	updates := datautils.Transform(nuggets, func(km *NewsNugget) any {
		beans := beanstore.VectorSearch(km.Embeddings, _CATEGORY_EMB,
			store.WithVectorFilter(store.JSON{
				"kind": store.JSON{"$in": []string{ARTICLE, POST}},
			}),
			store.WithProjection(_PROJECTION_FIELDS),
			store.WithMinSearchScore(_MIN_SCORE_VECTOR), //0.67 seems to be reasonably precise and not to lax
			store.WithVectorTopN(_TOPN_NUGGET_MAP))

		return NewsNugget{
			MatchCount: len(beans),
			BeanUrls:   datautils.Transform(beans, func(item *Bean) string { return item.Url }),
		}
	})
	// log.Println(updates)
	ids := datautils.Transform(nuggets, func(item *NewsNugget) store.JSON { return store.JSON{"_id": item.ID} })

	newsnuggetstore.Update(updates, ids)
}

func GetTrendingNewsNuggets(time_window int) []NewsNugget {
	time_window = checkAndFixTimeWindow(time_window)
	pipeline := []store.JSON{
		{
			"$match": store.JSON{
				"updated":     store.JSON{"$gte": timeValue(time_window)},
				"match_count": store.JSON{"$gte": 2}, // maybe I need to change the range
			},
		},
		// TODO: add some match based on the keyphrase
		// {
		// 	"$group": store.JSON{
		// 		"_id":   "$keyword",
		// 		"count": store.JSON{"$count": 1},
		// 	},
		// },
		{
			"$sort": store.JSON{"match_count": -1},
		},
		{
			"$project": store.JSON{
				"embeddings":  0,
				"mapped_urls": 0,
			},
		},
	}
	return newsnuggetstore.Aggregate(pipeline)
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

// this function uses large language model to generate all the custom fields and adds the to the DB
func generateCustomFields(beans []Bean, batch_update_time int64) {
	log.Println("Generating fields for a batch of", len(beans), "items")

	// get identifier and text content for processing
	filters := getBeanIdFilters(beans)
	texts := getTextFields(beans)

	// update beans with the generated fields
	datautils.ForEach(_GENERATED_FIELDS, func(field *string) { updateBeans(*field, texts, filters) })

	// extract key news nuggets and add them to the store
	generateNewsNuggets(texts, batch_update_time)

	// now that the news nuggets and the beans are added remap the contents
	remapNewsNuggets()
}

func generateNewsNuggets(texts []string, batch_update_time int64) {
	// extract key newsnuggets
	newsnuggets := datautils.Transform(pb_client.ExtractKeyConcepts(texts), NewKeyConcept)
	// generate embeddings for the descriptions so that we can match it with the documents
	nugget_descriptions := datautils.Transform(newsnuggets, func(item *NewsNugget) string { return item.Description })
	embs := emb_client.CreateBatchTextEmbeddings(nugget_descriptions, embeddings.CATEGORIZATION)
	// it is possible that embeddings generation failed even after retry.
	// if things failed no need to insert those items
	if len(embs) == len(newsnuggets) {
		for i := 0; i < len(newsnuggets); i++ {
			newsnuggets[i].Embeddings = embs[i]
			newsnuggets[i].Updated = batch_update_time
		}
		newsnuggetstore.Add(newsnuggets)
	}
}

func updateBeans(field_name string, texts []string, filters []store.JSON) {
	var updates []any

	switch field_name {
	case _CATEGORY_EMB:
		updates = datautils.Transform(texts, func(item *string) any {
			return Bean{CategoryEmbeddings: emb_client.CreateTextEmbeddings(*item, embeddings.CATEGORIZATION)}
		})

	case _SEARCH_EMB:
		updates = datautils.Transform(texts, func(item *string) any {
			return Bean{SearchEmbeddings: emb_client.CreateTextEmbeddings(*item, embeddings.SEARCH_DOCUMENT)}
		})

	case _SUMMARY:
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
