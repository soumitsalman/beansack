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

// default configurations
const (
	// content length for processing for NLP driver
	_MIN_TEXT_LENGTH = 20
	// rectification
	_RECT_BATCH_SIZE                 = 10
	_DEFAULT_NUGGET_MATCH_SCORE      = 0.73
	_DEFAULT_NUGGET_TEXT_MATCH_SCORE = 10
)

var _GENERATED_FIELDS = []string{_CATEGORY_EMB, _SEARCH_EMB, _SUMMARY}

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
		item.Updated = update_time
		item.Text = utils.TruncateTextOnTokenCount(item.Text)
		item.MediaNoise = nil
	})
	datautils.ForEach(medianoises, func(item *MediaNoise) {
		item.Updated = update_time
		item.Digest = utils.TruncateTextOnTokenCount(item.Digest)
	})

	// now store the beans before processing them for generated fields
	// for social media posts with media_noise update the update_time of the original post
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
	noisestore.Delete(delete_filter)
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
			store.WithMinSearchScore(_DEFAULT_NUGGET_MATCH_SCORE),
			store.WithVectorTopN(_MAX_TOPN),
			store.WithProjection(url_fields))
		// when vector search didn't pan out well do a text search and take the top 2
		if len(beans) == 0 {
			beans = beanstore.TextSearch([]string{km.KeyPhrase, km.Event},
				store.WithTextFilter(non_channels),
				store.WithMinSearchScore(_DEFAULT_NUGGET_TEXT_MATCH_SCORE),
				store.WithTextTopN(2), // i might have to change this
				store.WithProjection(url_fields))
		}
		// get media noises and add up the score to reflect in the Nugget Score

		return NewsNugget{
			TrendScore: calculateNuggetScore(beans), // score = 5 x number_of_unique_urls + sum (noise_score)
			BeanUrls:   datautils.Transform(beans, func(item *Bean) string { return item.Url }),
		}
	})
	// log.Println(updates)
	ids := datautils.Transform(nuggets, func(item *NewsNugget) store.JSON { return store.JSON{"_id": item.ID} })
	nuggetstore.Update(updates, ids)
}

// current calculation score: 5 x number_of_unique_articles_or_posts + sum_of(noise_scores)
func calculateNuggetScore(beans []Bean) int {
	var base = len(beans) * 5
	score := getMediaNoises(beans, true)
	if len(score) == 1 {
		base += score[0].Score
	}
	return base
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
