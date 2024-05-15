package sdk

import (
	"log"
	"time"

	"github.com/soumitsalman/beansack/nlp/embeddings"
	"github.com/soumitsalman/beansack/nlp/parrotbox"
	"github.com/soumitsalman/beansack/nlp/utils"
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

	// time windows
	_FOUR_WEEKS    = 28
	_ONE_DAY       = 1
	_DELETE_WINDOW = 15

	// vector and text search filters
	_DEFAULT_CATEGORY_SCORE = 0.70
	_DEFAULT_CONTEXT_SCORE  = 0.62
	_DEFAULT_SCORE_TEXT     = 10

	// rectification
	_RECT_BATCH_SIZE = 10
)

var (
	beanstore    *store.Store[Bean]
	keywordstore *store.Store[KeywordMap]
	nuggetstore  *store.Store[NewsNugget]
	noisestore   *store.Store[MediaNoise]
	emb_client   *embeddings.EmbeddingsDriver
	pb_client    *parrotbox.GoParrotboxClient
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
	_GENERATED_FIELDS = []string{_CATEGORY_EMB, _SEARCH_EMB, _SUMMARY}
	_SORT_BY_UPDATED  = store.JSON{"updated": -1}
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
	nuggetstore = store.New[NewsNugget](db_conn_str, BEANSACK, NEWSNUGGETS)

	if beanstore == nil || keywordstore == nil || nuggetstore == nil {
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
		item.Text = utils.TruncateTextOnTokenCount(item.Text)
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

func TextSearch(keywords []string, settings *SearchOptions) []Bean {
	return beanstore.TextSearch(keywords,
		store.WithTextFilter(settings.Filter),
		store.WithProjection(_PROJECTION_FIELDS),
		store.WithMinSearchScore(_DEFAULT_SCORE_TEXT),
		store.WithTextTopN(settings.TopN))
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
	return beanstore.Get(
		bean_filter,
		_PROJECTION_FIELDS,
		_SORT_BY_UPDATED, // this way the newest ones are listed first
		settings.TopN,
	)
}

func VectorSearch(options *SearchOptions) []Bean {
	var embs [][]float32
	var vec_field = _CATEGORY_EMB
	var min_score = _DEFAULT_CATEGORY_SCORE

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
		vec_field, min_score = _SEARCH_EMB, _DEFAULT_CONTEXT_SCORE
	} else {
		log.Println("[beanops] No `search` parameter defined.")
		return nil
	}

	return beanstore.VectorSearch(
		embs,
		vec_field,
		store.WithVectorFilter(options.Filter),
		store.WithProjection(_PROJECTION_FIELDS),
		store.WithMinSearchScore(min_score),
		store.WithVectorTopN(options.TopN))
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
func Rectify() {
	delete_filter := store.JSON{
		"updated": store.JSON{"$lte": timeValue(_DELETE_WINDOW)},
	}
	// delete old stuff
	beanstore.Delete(
		datautils.AppendMaps(
			delete_filter,
			store.JSON{
				"kind": store.JSON{"$ne": CHANNEL},
			}),
	)
	keywordstore.Delete(delete_filter)
	nuggetstore.Delete(delete_filter)

	// BEANS: generate the fields that do not exist
	for _, field_name := range _GENERATED_FIELDS {
		beans := beanstore.Get(
			store.JSON{
				field_name: store.JSON{"$exists": false},
				"updated":  store.JSON{"$gte": timeValue(2)},
				"kind":     store.JSON{"$ne": CHANNEL},
			},
			store.JSON{
				"url":  1,
				"text": 1,
			},
			_SORT_BY_UPDATED, // this way the newest ones get priority
			-1,
		)
		log.Printf("[dbops] Rectifying %s for %d items\n", field_name, len(beans))

		// store generated field
		rectifyBeans(beans, field_name)
	}

	// NUGGETS: generate embeddings for the ones that do not yet have it
	// process data in batches so that there is at least partial success
	// it is possible that embeddings generation failed even after retry.
	// if things failed no need to insert those items
	nuggets := nuggetstore.Get(
		store.JSON{
			"embeddings": store.JSON{"$exists": false},
			"updated":    store.JSON{"$gte": timeValue(2)},
		},
		store.JSON{
			"_id":         1,
			"description": 1,
		},
		_SORT_BY_UPDATED, // this way the newest ones get priority
		-1,
	)
	rectifyNewsNuggets(nuggets)

	// MAPPING: now that the beans and nuggets have embeddings, remap them
	rectifyNuggetMapping()
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
			store.WithMinSearchScore(_DEFAULT_CATEGORY_SCORE),
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
			store.WithMinSearchScore(_DEFAULT_CATEGORY_SCORE),
			store.WithVectorFilter(query.Filter),
			store.WithProjection(projection),
			store.WithVectorTopN(query.TopN),
			store.WithSortBy(sort_by),
		)
	} else {
		return nuggetstore.Get(query.Filter, projection, sort_by, query.TopN)
	}
}

// this function uses large language model to generate all the custom fields and adds the to the DB
func generateCustomFields(beans []Bean, batch_update_time int64) {
	// extract key news nuggets and add them to the store
	generateNewsNuggets(getTextFields(beans), batch_update_time)
	// run rectification and re-adjustment of all items that need updating
	Rectify()
}

func generateNewsNuggets(texts []string, batch_update_time int64) {
	// extract key newsnuggets
	newsnuggets := datautils.Transform(pb_client.ExtractKeyConcepts(texts), NewKeyConcept)
	newsnuggets = datautils.ForEach(newsnuggets, func(item *NewsNugget) { item.Updated = batch_update_time })
	nuggetstore.Add(newsnuggets)
}

func rectifyBeans(beans []Bean, field_name string) {
	log.Printf("[beanops] Generating %s for a batch of %d beans", field_name, len(beans))

	runInBatches(beans, _RECT_BATCH_SIZE, func(batch []Bean) {
		texts := getTextFields(batch)
		// generate whatever needs to be generated
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
		// get identifier and text content for processing
		filters := getBeanIdFilters(batch)
		beanstore.Update(updates, filters)
	})
}

func rectifyNewsNuggets(nuggets []NewsNugget) {
	log.Printf("[beanops] Generating embeddings for %d News Nuggets.\n", len(nuggets))
	runInBatches(nuggets, _RECT_BATCH_SIZE, func(batch []NewsNugget) {
		descriptions := datautils.Transform(batch, func(item *NewsNugget) string { return item.Description })
		embs := datautils.Transform(
			emb_client.CreateBatchTextEmbeddings(descriptions, embeddings.CATEGORIZATION),
			func(item *[]float32) any {
				return NewsNugget{Embeddings: *item}
			})

		if len(embs) == len(descriptions) {
			ids := datautils.Transform(batch, func(item *NewsNugget) store.JSON { return store.JSON{"_id": item.ID} })
			nuggetstore.Update(embs, ids)
		}
	})
}

func rectifyNuggetMapping() {
	nuggets := nuggetstore.Get(
		store.JSON{
			"embeddings": store.JSON{"$exists": true}, // ignore if a nugget if it doesnt have an embedding
		},
		nil, nil, -1)

	url_fields := store.JSON{"url": 1}
	non_channels := store.JSON{
		"kind": store.JSON{"$ne": CHANNEL},
	}
	updates := datautils.Transform(nuggets, func(km *NewsNugget) any {
		// search with vector embedding
		// this is still a fuzzy search and it does not always work well
		// if it doesn't do a text search
		beans := beanstore.VectorSearch([][]float32{km.Embeddings},
			_CATEGORY_EMB,
			store.WithVectorFilter(non_channels),
			store.WithMinSearchScore(_DEFAULT_CATEGORY_SCORE), //0.67 seems to be reasonably precise and not to lax
			store.WithVectorTopN(_MAX_TOPN),
			store.WithProjection(url_fields))
		// when vector search didn't pan out well do a text search and take the top 2
		if len(beans) == 0 {
			beans = beanstore.TextSearch([]string{km.KeyPhrase, km.Event},
				store.WithTextFilter(non_channels),
				store.WithMinSearchScore(_DEFAULT_SCORE_TEXT),
				store.WithTextTopN(2), // i might have to change this
				store.WithProjection(url_fields))
		}
		return NewsNugget{
			MatchCount: len(beans),
			BeanUrls:   datautils.Transform(beans, func(item *Bean) string { return item.Url }),
		}
	})
	// log.Println(updates)
	ids := datautils.Transform(nuggets, func(item *NewsNugget) store.JSON { return store.JSON{"_id": item.ID} })

	nuggetstore.Update(updates, ids)
}

func runInBatches[T any](items []T, batch_size int, do func(batch []T)) {
	for i := 0; i < len(items); i += batch_size {
		do(datautils.SafeSlice(items, i, i+batch_size))
	}
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
