package sdk

import (
	"log"

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
	// time windows
	_FOUR_WEEKS    = 28
	_ONE_DAY       = 1
	_DELETE_WINDOW = 15

	// vector and text search filters
	_DEFAULT_CATEGORY_MATCH_SCORE = 0.67
	_DEFAULT_CONTEXT_MATCH_SCORE  = 0.62
	_DEFAULT_TEXT_MATCH_SCORE     = 10
)

var (
	beanstore   *store.Store[Bean]
	nuggetstore *store.Store[NewsNugget]
	noisestore  *store.Store[MediaNoise]
	emb_client  *embeddings.EmbeddingsDriver
	pb_client   *parrotbox.GoParrotboxClient
)

const (
	_SEARCH_EMB   = "search_embeddings"
	_CATEGORY_EMB = "category_embeddings"
	_SUMMARY      = "summary"
)

var (
	_PROJECTION_FIELDS = store.JSON{
		// for beans
		"url":          1,
		"updated":      1,
		"source":       1,
		"title":        1,
		"kind":         1,
		"author":       1,
		"created":      1,
		"summary":      1,
		"keywords":     1,
		"topic":        1,
		"search_score": 1,

		// for media noise
		"score":     1,
		"sentiment": 1,

		// for concepts
		"keyphrase":   1,
		"description": 1,
	}
	_SORT_BY_UPDATED = store.JSON{"updated": -1}
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
	nuggetstore = store.New[NewsNugget](db_conn_str, BEANSACK, NEWSNUGGETS)

	if beanstore == nil || nuggetstore == nil {
		return BeanSackError("Initialization Failed. db_conn_str Not working.")
	}

	pb_client = parrotbox.NewGoParrotboxClient(parrotbox_auth_token)
	emb_client = embeddings.NewEmbeddingsDriver()

	return nil
}

func TextSearch(keywords []string, settings *SearchOptions) []Bean {
	return attachMediaNoises(beanstore.TextSearch(keywords,
		store.WithTextFilter(settings.Filter),
		store.WithProjection(_PROJECTION_FIELDS),
		store.WithMinSearchScore(_DEFAULT_TEXT_MATCH_SCORE),
		store.WithTextTopN(settings.TopN)))
}

func NuggetSearch(nuggets []string, settings *SearchOptions) []Bean {
	// get all the mapped urls
	nuggets_filter := store.JSON{
		"keyphrase": store.JSON{"$in": nuggets},
	}
	if updated, ok := settings.Filter["updated"]; ok {
		nuggets_filter["updated"] = updated
	}
	initial_list := nuggetstore.Get(nuggets_filter, store.JSON{"mapped_urls": 1}, store.JSON{"match_count": -1}, settings.TopN)

	// merge mapped_urls into one array
	mapped_urls := make([]string, 0, len(initial_list)*5)
	datautils.ForEach(initial_list, func(item *NewsNugget) { mapped_urls = append(mapped_urls, item.BeanUrls...) })

	// find the news articles with the urls in scope
	bean_filter := store.JSON{
		"url": store.JSON{"$in": mapped_urls},
	}
	if kind, ok := settings.Filter["kind"]; ok {
		bean_filter["kind"] = kind
	}
	beans := beanstore.Get(
		bean_filter,
		_PROJECTION_FIELDS,
		_SORT_BY_UPDATED, // this way the newest ones are listed first
		settings.TopN,
	)
	return attachMediaNoises(beans)
}

func VectorSearch(options *SearchOptions) []Bean {
	var embs [][]float32
	var vec_field = _CATEGORY_EMB
	var min_score = _DEFAULT_CATEGORY_MATCH_SCORE

	if len(options.SearchEmbeddings) > 0 {
		// no need to generate embeddings. search for CATEGORIES defined by these
		embs = options.SearchEmbeddings
	} else if len(options.SearchCategories) > 0 {
		// generate embeddings for these categories
		log.Printf("[dbops] Generating embeddings for %d categories.\n", len(options.SearchCategories))
		embs = emb_client.CreateBatchTextEmbeddings(options.SearchCategories, embeddings.CATEGORIZATION)
	} else if len(options.SearchContext) > 0 {
		// generate embeddings for the context and search using SEARCH EMBEDDINGS
		log.Println("[dbops] Generating embeddings for:", options.SearchContext)
		embs = [][]float32{emb_client.CreateTextEmbeddings(options.SearchContext, embeddings.SEARCH_QUERY)}
		vec_field, min_score = _SEARCH_EMB, _DEFAULT_CONTEXT_MATCH_SCORE
	} else {
		log.Println("[beanops] No `search` parameter defined.")
		return nil
	}
	beans := beanstore.VectorSearch(
		embs,
		vec_field,
		store.WithVectorFilter(options.Filter),
		store.WithProjection(_PROJECTION_FIELDS),
		store.WithMinSearchScore(min_score),
		store.WithVectorTopN(options.TopN))
	return attachMediaNoises(beans)
}

func TrendingNuggets(query *SearchOptions) []NewsNugget {
	query.Filter["match_count"] = store.JSON{"$gte": 1}
	projection := store.JSON{
		"embeddings":  0,
		"mapped_urls": 0,
		"_id":         0,
	}
	sort_by := store.JSON{"match_count": -1}

	if len(query.SearchEmbeddings) > 0 {
		return nuggetstore.VectorSearch(
			query.SearchEmbeddings,
			"embeddings",
			store.WithMinSearchScore(_DEFAULT_NUGGET_MATCH_SCORE),
			store.WithVectorFilter(query.Filter),
			store.WithProjection(projection),
			store.WithVectorTopN(query.TopN),
			store.WithSortBy(sort_by),
		)
	} else if len(query.SearchCategories) > 0 {
		log.Printf("[beanops] Creating embeddings for %d categories.\n", len(query.SearchCategories))
		return nuggetstore.VectorSearch(
			emb_client.CreateBatchTextEmbeddings(query.SearchCategories, embeddings.CATEGORIZATION),
			"embeddings",
			store.WithMinSearchScore(_DEFAULT_NUGGET_MATCH_SCORE),
			store.WithVectorFilter(query.Filter),
			store.WithProjection(projection),
			store.WithVectorTopN(query.TopN),
			store.WithSortBy(sort_by),
		)
	} else {
		return nuggetstore.Get(query.Filter, projection, sort_by, query.TopN)
	}
}

func attachMediaNoises(beans []Bean) []Bean {
	noises := getMediaNoises(beans, false)
	if len(noises) > 0 {
		beans = datautils.ForEach(beans, func(bn *Bean) {
			i := datautils.IndexAny(noises, func(mn *MediaNoise) bool { return bn.Url == mn.BeanUrl })
			if i >= 0 {
				bn.MediaNoise = &noises[i]
			}
		})
	}
	return beans
}

func getMediaNoises(beans []Bean, total bool) []MediaNoise {
	if len(beans) == 0 {
		return nil
	}
	urls := datautils.Transform(beans, func(item *Bean) string { return item.Url })
	// log.Println(datautils.ToJsonString(urls))
	pipeline := []store.JSON{
		{
			"$match": store.JSON{
				"mapped_url": store.JSON{"$in": urls},
			},
		},
		{
			"$sort": store.JSON{"updated": -1},
		},
		{
			"$group": store.JSON{
				"_id": store.JSON{
					"mapped_url": "$mapped_url",
					"source":     "$source",
					"channel":    "$channel",
				},
				"updated":       store.JSON{"$first": "$updated"},
				"mapped_url":    store.JSON{"$first": "$mapped_url"},
				"channel":       store.JSON{"$first": "$channel"},
				"container_url": store.JSON{"$first": "$container_url"},
				"likes":         store.JSON{"$first": "$likes"},
				"comments":      store.JSON{"$first": "$comments"},
			},
		},
		{
			"$group": store.JSON{
				"_id":           "$mapped_url",
				"updated":       store.JSON{"$first": "$updated"},
				"mapped_url":    store.JSON{"$first": "$mapped_url"},
				"channel":       store.JSON{"$first": "$channel"},
				"container_url": store.JSON{"$first": "$container_url"},
				"likes":         store.JSON{"$sum": "$likes"},
				"comments":      store.JSON{"$sum": "$comments"},
			},
		},
		{
			"$project": store.JSON{
				"mapped_url":    1,
				"channel":       1,
				"container_url": 1,
				"likes":         1,
				"comments":      1,
				"score": store.JSON{
					"$add": []any{
						store.JSON{"$multiply": []any{"$comments", 3}},
						"$likes",
					},
				},
			},
		},
	}
	if total {
		pipeline = append(pipeline, store.JSON{
			"$group": store.JSON{
				"_id":   nil,
				"score": store.JSON{"$sum": "$score"},
			},
		})
	}
	return noisestore.Aggregate(pipeline)
}
