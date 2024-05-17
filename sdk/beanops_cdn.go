package sdk

import (
	"log"
	"sort"

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
	_DEFAULT_CATEGORY_MATCH_SCORE = 0.7
	_DEFAULT_CONTEXT_MATCH_SCORE  = 0.55
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

// fuzzy search modes
const (
	_GET            = 0
	_TEXT           = 1
	_VECTOR         = 2
	_VECTOR_OR_TEXT = 3
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
	return attachMediaNoises(
		beanstore.TextSearch(keywords,
			store.WithTextFilter(settings.Filter),
			store.WithProjection(_PROJECTION_FIELDS),
			store.WithTextTopN(settings.TopN)))
}

// Searches beans based on search options
// Algorithm:
//  1. Look for category embeddings in input. If so, category search with that
//  2. If NO category embeddings are found then create embeddings from category texts and search with those
//  3. If NO category texts are found then create embeddings from the conversational context and search with that
//     3.ALT. If context search does not return a value to a TextSearch
//  4. If NO vector input is available just do a regular search
func FuzzySearchBeans(options *SearchOptions) []Bean {
	mode, embs, vec_field, min_score, keywords := getFuzzySearchMode(options)
	var beans []Bean

	switch mode {
	case _GET:
		beans = beanstore.Get(
			options.Filter,
			_PROJECTION_FIELDS,
			store.JSON{"updated": -1},
			options.TopN)
	case _TEXT:
		beans = TextSearch(keywords, options)
	case _VECTOR:
		beans = beanstore.VectorSearch(
			embs,
			vec_field,
			store.WithVectorFilter(options.Filter),
			store.WithProjection(_PROJECTION_FIELDS),
			store.WithMinSearchScore(min_score),
			store.WithVectorTopN(options.TopN))
	case _VECTOR_OR_TEXT:
		beans = beanstore.VectorSearch(
			embs,
			vec_field,
			store.WithVectorFilter(options.Filter),
			store.WithProjection(_PROJECTION_FIELDS),
			store.WithMinSearchScore(min_score),
			store.WithVectorTopN(options.TopN))
		// vector search score is too restrictive for the embeddings model
		// do a text search and return the top 2 as sample
		if len(beans) <= 0 {
			options.TopN = 2
			beans = TextSearch(keywords, options)
		}
	}
	return attachMediaNoises(beans)
}

// gets parameters for fuzzy search.
// the outputs are: search_mode, embeddings (if applicable), vector_field (if applicable), min_vector_search_score, keywords (if applicable)
func getFuzzySearchMode(options *SearchOptions) (int, [][]float32, string, float64, []string) {
	var embs [][]float32
	if len(options.CategoryEmbeddings) > 0 {
		// no need to generate embeddings. search for CATEGORIES defined by these
		return _VECTOR, options.CategoryEmbeddings, _CATEGORY_EMB, _DEFAULT_CATEGORY_MATCH_SCORE, []string{""}
	} else if len(options.CategoryTexts) > 0 {
		// generate embeddings for these categories
		log.Printf("[beanops] Generating embeddings for %d categories.\n", len(options.CategoryTexts))
		embs = emb_client.CreateBatchTextEmbeddings(options.CategoryTexts, embeddings.CATEGORIZATION)
		return _VECTOR, embs, _CATEGORY_EMB, _DEFAULT_CATEGORY_MATCH_SCORE, options.CategoryTexts
	} else if len(options.Context) > 0 {
		// generate embeddings for the context and search using SEARCH EMBEDDINGS
		log.Println("[beanops] Generating embeddings for:", options.Context)
		embs = [][]float32{emb_client.CreateTextEmbeddings(options.Context, embeddings.SEARCH_QUERY)}
		return _VECTOR_OR_TEXT, embs, _SEARCH_EMB, _DEFAULT_CONTEXT_MATCH_SCORE, []string{options.Context}
	} else {
		log.Println("[beanops] No `vector search` parameter defined.")
		return _GET, nil, "", 0, nil // none of the other parameters matter
	}
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

// Finds the trending news nuggets defined by the search parameter such as: by the day/week, by category match
// Algorithm:
//  0. (Optional) Find all the nuggets in that day/week and get their urls
//  1. Match all the beans irrespective of updated: 0/1 within the category match threshold
//  2. Find the nuggets that has those URLs as mapped urls for that day
//  3. Stack rank them by trend score
func TrendingNuggets(options *SearchOptions) []NewsNugget {
	// 0. Find all nuggets in that day/week
	nugget_filter := store.JSON{
		"match_count": store.JSON{"$gte": 1}, // this a minimum
	}
	if updated, ok := options.Filter["updated"]; ok {
		nugget_filter["updated"] = updated
	}
	initial_urls := make([]string, 0, 10) //default initialization
	datautils.ForEach(
		nuggetstore.Get(nugget_filter, store.JSON{"mapped_urls": 1}, nil, -1),
		func(item *NewsNugget) { initial_urls = append(initial_urls, item.BeanUrls...) })
	// there is nothing for the day
	if len(initial_urls) <= 0 {
		return nil
	}
	log.Println(len(initial_urls))

	// 1. Match the all beans irrespective of updated: 0/1 within category match
	beans_options := *options
	beans_options.Filter = store.JSON{"url": store.JSON{"$in": initial_urls}}
	beans_options.TopN = len(initial_urls) // look for all the items that match and dont shorten to only user provided topN just yet
	matched_urls := datautils.Transform(FuzzySearchBeans(&beans_options), func(item *Bean) string { return item.Url })
	// there is nothing that matches the categories
	if len(matched_urls) <= 0 {
		return nil
	}
	log.Println(len(matched_urls))

	// 2. Find the nuggets that has those URLs as mapped urls for that day
	// 3. Stack rank them by trend score
	nugget_filter["mapped_urls"] = store.JSON{"$in": matched_urls} // now find the ones with matched urls
	return nuggetstore.Get(
		nugget_filter,
		store.JSON{
			"embeddings":  0,
			"mapped_urls": 0,
			"_id":         0,
		},
		store.JSON{"match_count": -1}, // stack rank by trend score
		options.TopN,                  // now add the topN provided by user
	)
}

// Returns the trending news/posts defined by the search parameter such as: by the day/week, by category match
// Algorithm:
//  1. Find all the news/posts for that day that matches the categories (match everything if there is no category)
//  2. Find the nuggets that are mapped to these articles
//  3. Take the highest nugget trend score and assign to the respective article
//  4. Stack rank the news/posts by that trend score
func TrendingBeans(options *SearchOptions) []Bean {
	//  1. Find all the news/posts for that day that matches the categories (match everything if there is no category)
	beans := FuzzySearchBeans(options)

	//  2. Find the nuggets that are mapped to these articles
	urls := datautils.Transform(beans, func(item *Bean) string { return item.Url })
	nuggets := nuggetstore.Aggregate([]store.JSON{
		{
			"$match": store.JSON{
				"mapped_urls": store.JSON{"$in": urls},
			},
		},
		{
			"$sort": store.JSON{"match_count": -1},
		},
		{
			"$unwind": "$mapped_urls",
		},
		{
			"$project": store.JSON{
				"match_count": 1,
				"url":         "$mapped_urls",
			},
		},
		{
			"$group": store.JSON{
				"_id": "$url",
				// HACK: using keywords as a holder for $url is a hack
				"keyphrase":   store.JSON{"$first": "$url"},
				"match_count": store.JSON{"$first": "$match_count"},
			},
		},
	})

	// if no nugget was found just return based on search score of the beans
	if len(nuggets) > 0 {
		//  3. Take the highest nugget trend score and assign to the respective article
		beans = datautils.ForEach(beans, func(bn *Bean) {
			i := datautils.IndexAny(nuggets, func(nug *NewsNugget) bool { return bn.Url == nug.KeyPhrase })
			if i >= 0 {
				bn.SearchScore = float64(nuggets[i].TrendScore)
			}
		})

		//  4. Stack rank the news/posts by that trend score
		sort.Slice(beans, func(i, j int) bool { return beans[i].SearchScore > beans[j].SearchScore })
		beans = datautils.SafeSlice(beans, 0, options.TopN)
	}
	return attachMediaNoises(beans)
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
