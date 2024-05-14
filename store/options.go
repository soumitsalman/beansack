package store

import (
	"strings"

	datautils "github.com/soumitsalman/data-utils"
)

const (
	_DEFAULT_SEARCH_MIN_SCORE = 0.5
	_DEFAULT_SEARCH_TOP_N     = 5
)

type StoreOption[T any] func(store *Store[T])
type SearchOption func(search_pipeline []JSON)

func WithDataIDAndEqualsFunction[T any](id_func func(data *T) JSON, equals func(a, b *T) bool) StoreOption[T] {
	return func(store *Store[T]) {
		store.get_id = id_func
		store.equals = equals
	}
}

// scalar filter is part of the first item in the pipeline
func WithVectorFilter(filter JSON) SearchOption {
	return func(search_pipeline []JSON) {
		if len(filter) > 0 {
			search_pipeline[0]["$search"].(JSON)["cosmosSearch"].(JSON)["filter"] = filter
		}
	}
}

// scalar filter is part of the first item in the pipeline
func WithTextFilter(filter JSON) SearchOption {
	return func(search_pipeline []JSON) {
		if len(filter) > 0 {
			datautils.AppendMaps(
				search_pipeline[0]["$match"].(JSON),
				filter)
		}
	}
}

// topN is part of the first item in the pipeline
func WithVectorTopN(top_n int) SearchOption {
	return func(search_pipeline []JSON) {
		search_pipeline[0]["$search"].(JSON)["cosmosSearch"].(JSON)["k"] = top_n
	}
}

// this is the second item in the pipeline
// this applies to both
func WithProjection(fields JSON) SearchOption {
	return func(search_pipeline []JSON) {
		if len(fields) > 0 {
			datautils.AppendMaps(
				// $project is the last item
				search_pipeline[1]["$project"].(JSON),
				fields)
		}
	}
}

// this is in the 3rd item of the pipeline
// this applies to both tecxt and vector search
func WithMinSearchScore(score float64) SearchOption {
	return func(search_pipeline []JSON) {
		search_pipeline[2]["$match"].(JSON)["search_score"].(JSON)["$gte"] = score
	}
}

// topN is part of the last item in the pipeline for text search
func WithTextTopN(top_n int) SearchOption {
	return func(search_pipeline []JSON) {
		search_pipeline[len(search_pipeline)-1]["$limit"] = top_n
	}
}

func createDefaultVectorSearchPipeline(query_embeddings []float32, vector_field string) []JSON {
	return []JSON{
		{
			"$search": JSON{
				"cosmosSearch": JSON{
					"vector": query_embeddings,
					"path":   vector_field,
					"k":      _DEFAULT_SEARCH_TOP_N,
				},
				"returnStoredSource": true,
			},
		},
		{
			"$project": JSON{
				"search_score": JSON{"$meta": "searchScore"},
				"_id":          1,
			},
		},
		{
			"$match": JSON{
				"search_score": JSON{"$gte": _DEFAULT_SEARCH_MIN_SCORE},
			},
		},
	}
}

func createDefaultTextSearchPipeline(query_texts []string) []JSON {
	return []JSON{
		{
			"$match": JSON{
				"$text": JSON{"$search": strings.Join(query_texts, " ")},
			},
		},
		{
			"$project": JSON{
				"search_score": JSON{"$meta": "textScore"},
				"_id":          1,
			},
		},
		{
			"$match": JSON{
				"search_score": JSON{"$gt": 1}, // this is the default min
			},
		},
		{
			"$sort": JSON{"search_score": -1},
		},
		{
			"$limit": _DEFAULT_SEARCH_TOP_N,
		},
	}
}
