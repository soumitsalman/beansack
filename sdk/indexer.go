package sdk

import (
	"log"
	"time"

	"github.com/soumitsalman/beansack/nlp"
	"github.com/soumitsalman/beansack/store"
	datautils "github.com/soumitsalman/data-utils"
)

const (
	_MIN_RECTIFY_WINDOW = 2
	_MAX_RECTIFY_WINDOW = 30
)

// default configurations
const (
	_MIN_TEXT_LENGTH                 = 100 // content length for processing for NLP driver
	_RECT_BATCH_SIZE                 = 10  // rectification
	_DEFAULT_NUGGET_MATCH_SCORE      = 0.73
	_DEFAULT_NUGGET_TEXT_MATCH_SCORE = 10
)

// var _GENERATED_FIELDS = []string{_CATEGORY_EMB, _SEARCH_EMB, _SUMMARY}
// removing search embeddings
var _GENERATED_FIELDS = []string{_CLASSIFICATION_EMB, _SUMMARY}

func Cleanup(delete_window int) {
	delete_filter := store.JSON{
		"updated": store.JSON{"$lte": timeValue(delete_window)},
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
}

// Adding feeds from news sources and social media
// Steps:
//  1. Filter out the tiny ones for now
//  2. Truncate the contents to keep below the limit
//  3. Add the beans to the database
//  4. Add media noise to database
//  5. Create news nuggets and add to db
//  6. Create embeddings for news nuggets and add to db
//  7. Create generated fields for the beans and add them to database
//  8. Map the news nuggets to the beans
func AddBeans(beans []Bean) {
	// 1. Filter out the tiny ones and the channels for now
	beans = datautils.Filter(beans, func(item *Bean) bool { return (len(item.Text) >= _MIN_TEXT_LENGTH) && (item.Kind != CHANNEL) })

	// extract out the beans medianoises
	medianoises := datautils.FilterAndTransform(beans, func(item *Bean) (bool, MediaNoise) {
		if item.MediaNoise != nil {
			item.MediaNoise.BeanUrl = item.Url
			return true, *item.MediaNoise
		} else {
			return false, MediaNoise{}
		}
	})

	// 2. Truncate the contents to keep below the limit and assign update time
	update_time := time.Now().Unix()
	beans = datautils.ForEach(beans, func(item *Bean) {
		item.Updated = update_time
		item.Text = nlp.TruncateTextOnTokenCount(item.Text)
		item.MediaNoise = nil
	})

	// 3. Add the beans to the database
	// notice that the beans get reassigned for custom fields generation
	// since if certain bean does not get added it has already been processed and linked
	beans, err := beanstore.Add(beans)
	if err != nil {
		log.Println("[beansack|Indexer] Failed to add new beans. Terminating early.", err)
		return
	}

	// 4. Add media noise to database
	if len(medianoises) > 0 {
		// If a bean with the same url exists it will not get added but if it has a media noise it should get updated with the current updated date
		beans_update := make([]any, 0, len(medianoises))
		beans_ids := make([]store.JSON, 0, len(medianoises))

		datautils.ForEach(medianoises, func(item *MediaNoise) {
			item.Updated = update_time
			item.Digest = nlp.TruncateTextOnTokenCount(item.Digest)
			// create the update times for the beans
			beans_update = append(beans_update, store.JSON{"updated": update_time})
			beans_ids = append(beans_ids, store.JSON{"url": item.BeanUrl})
		})
		// now store the medianoises. But no need to check for error since their storage is auxiliary for the overall experience
		noisestore.Add(medianoises)
		// update the beans with medianoise
		beanstore.Update(beans_update, beans_ids)
	}

	// if no new bean got added then no need to go through hoops for these
	if len(beans) > 0 {
		// 5. Create news nuggets and add to db
		// 6. Create embeddings for news nuggets and add to db
		// parallelizing this one since its a different server than the embeddings
		// this will be faster than going through the custom fields
		go generateNewsNuggets(beans)

		// 7. Create generated fields for the beans and add them to database
		generateCustomFieldsForBeans(beans)

		// 8. Map the news nuggets to the beans
		// this is remap across the board that will take place for each Add Beans to keep the mapping fresh
		// even if not all the nuggets have been generated the new incoming nuggests will get mapped during the next rounds
		// this can happen in parallel and does not need to block the call
		go remapNewsNuggets(_MIN_RECTIFY_WINDOW)
	}
}

func generateCustomFieldsForBeans(beans []Bean) {
	datautils.ForEach(_GENERATED_FIELDS, func(field_name *string) { generateFieldForBeans(beans, *field_name) })
}

func generateFieldForBeans(beans []Bean, field_name string) {
	log.Printf("[beanops] Generating %s for a batch of %d beans", field_name, len(beans))

	// get identifier and text content for processing
	filters := getBeanIdFilters(beans)
	texts := getTextFields(beans)
	// generate whatever needs to be generated
	var updates []any
	switch field_name {
	case _CLASSIFICATION_EMB:
		cat_embs := emb_client.CreateBatchTextEmbeddings(texts, nlp.CLASSIFICATION)
		updates = datautils.Transform(cat_embs, func(emb *[]float32) any {
			return Bean{CategoryEmbeddings: *emb}
		})
	// case _SEARCH_EMB:
	// 	search_embs := emb_client.CreateBatchTextEmbeddings(texts, nlp.SEARCH_DOCUMENT)
	// 	updates = datautils.Transform(search_embs, func(emb *[]float32) any {
	// 		return Bean{SearchEmbeddings: *emb}
	// 	})
	case _SUMMARY:
		// summary and topic. but topic is low priority field and it comes with summary
		digests := pb_client.ExtractDigests(texts)
		updates = datautils.Transform(digests, func(item *nlp.Digest) any { return item })
	}
	beanstore.Update(updates, filters)
}

func generateNewsNuggets(beans []Bean) {
	// extract key newsnuggets
	keyconcepts := pb_client.ExtractKeyConcepts(getTextFields(beans))
	// remove the duds
	nuggets := datautils.FilterAndTransform(keyconcepts, func(keyconcept *nlp.KeyConcept) (bool, NewsNugget) {
		nugget := toNewsNugget(keyconcept)
		if len(keyconcept.Description) == 0 {
			// don't do anything if it is a dud
			return false, nugget
		}
		nugget.Updated = beans[0].Updated // update with time frame to associate to the beans
		return true, nugget
	})
	if len(keyconcepts) > len(nuggets) {
		log.Printf("[beanops] KeyConcepts generation returned %d duds.\n", len(keyconcepts)-len(nuggets))
	}

	// generate the embeddings
	log.Printf("[beanops] Generating embeddings for %d News Nuggets.\n", len(nuggets))
	descriptions := datautils.Transform(nuggets, func(item *NewsNugget) string { return item.Description })
	// deprecating categorization
	// embs := emb_client.CreateBatchTextEmbeddings(descriptions, nlp.CATEGORIZATION)
	embs := emb_client.CreateBatchTextEmbeddings(descriptions, nlp.SEARCH_QUERY)
	for i := range nuggets {
		nuggets[i].Embeddings = embs[i]
	}

	// now store the nuggets
	nuggetstore.Add(nuggets)
}

func generateCustomFieldForNuggets(nuggets []NewsNugget) {
	log.Printf("[beanops] Generating embeddings for %d News Nuggets.\n", len(nuggets))

	descriptions := datautils.Transform(nuggets, func(item *NewsNugget) string { return item.Description })
	embs := datautils.Transform(
		emb_client.CreateBatchTextEmbeddings(descriptions, nlp.CLASSIFICATION),
		func(item *[]float32) any {
			return NewsNugget{Embeddings: *item}
		})

	if len(embs) == len(descriptions) {
		ids := getNewsNuggetIds(nuggets)
		nuggetstore.Update(embs, ids)
	}
}

func remapNewsNuggets(window int) {
	nuggets := nuggetstore.Get(
		store.JSON{
			"embeddings": store.JSON{"$exists": true}, // ignore if a nugget if it doesnt have an embedding
			"updated":    store.JSON{"$gte": timeValue(window)},
		},
		store.JSON{
			"_id":        1,
			"embeddings": 1,
		}, nil, -1)

	url_fields := store.JSON{"url": 1}
	non_channels := store.JSON{
		"kind": store.JSON{"$ne": CHANNEL},
	}
	updates := datautils.Transform(nuggets, func(km *NewsNugget) any {
		// search with vector embedding
		// this is still a fuzzy search and it does not always work well
		// if it doesn't do a text search
		beans := beanstore.VectorSearch([][]float32{km.Embeddings},
			_CLASSIFICATION_EMB,
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
	ids := getNewsNuggetIds(nuggets)
	nuggetstore.Update(updates, ids)
}

// this is for any recurring service
// this is currently not being run as a recurring service
func Rectify() {
	// BEANS: generate the fields that do not exist
	for _, field_name := range _GENERATED_FIELDS {
		beans := beanstore.Get(
			store.JSON{
				field_name: store.JSON{"$exists": false},
				"updated":  store.JSON{"$gte": timeValue(_MAX_RECTIFY_WINDOW)},
				"kind":     store.JSON{"$ne": CHANNEL},
			},
			store.JSON{
				"url":  1,
				"text": 1,
			},
			_SORT_BY_UPDATED, // this way the newest ones get priority
			-1,
		)
		// store generated field
		generateFieldForBeans(beans, field_name)
	}

	// TODO: if certain bean doesn't have a nugget regenerate then

	// NUGGETS: generate embeddings for the ones that do not yet have it
	// process data in batches so that there is at least partial success
	// it is possible that embeddings generation failed even after retry.
	// if things failed no need to insert those items
	nuggets := nuggetstore.Get(
		store.JSON{
			"embeddings": store.JSON{"$exists": false},
			"updated":    store.JSON{"$gte": timeValue(_MAX_RECTIFY_WINDOW)},
		},
		store.JSON{
			"_id":         1,
			"description": 1,
		},
		_SORT_BY_UPDATED, // this way the newest ones get priority
		-1,
	)
	generateCustomFieldForNuggets(nuggets)
	// MAPPING: now that the beans and nuggets have embeddings, remap them
	remapNewsNuggets(_MAX_RECTIFY_WINDOW)
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

func getNewsNuggetIds(batch []NewsNugget) []store.JSON {
	// update it with updater
	ids := datautils.Transform(batch, func(item *NewsNugget) store.JSON {
		if item.ID == nil {
			// these have not been inserted so use the updated field
			return store.JSON{"updated": item.Updated}
		}
		return store.JSON{"_id": item.ID}
	})
	return ids
}
