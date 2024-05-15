package store

import (
	"strings"

	datautils "github.com/soumitsalman/data-utils"
)

const (
	_DEFAULT_SEARCH_TOP_N = 5
)

type StoreOption[T any] func(store *Store[T])
type SearchOption func(search_pipeline []JSON) []JSON

func WithDataIDAndEqualsFunction[T any](id_func func(data *T) JSON, equals func(a, b *T) bool) StoreOption[T] {
	return func(store *Store[T]) {
		store.get_id = id_func
		store.equals = equals
	}
}

// scalar filter is part of the first item in the pipeline
func WithVectorFilter(filter JSON) SearchOption {
	return func(search_pipeline []JSON) []JSON {
		if len(filter) > 0 {
			search_pipeline[0]["$search"].(JSON)["cosmosSearch"].(JSON)["filter"] = filter
		}
		return search_pipeline
	}
}

// scalar filter is part of the first item in the pipeline
func WithTextFilter(filter JSON) SearchOption {
	return func(search_pipeline []JSON) []JSON {
		if len(filter) > 0 {
			datautils.AppendMaps(
				search_pipeline[0]["$match"].(JSON),
				filter)
		}
		return search_pipeline
	}
}

func WithSortBy(sort_by JSON) SearchOption {
	return func(search_pipeline []JSON) []JSON {
		search_pipeline = append(search_pipeline, JSON{"$sort": sort_by})
		return search_pipeline
	}
}

// topN is part of the first item in the pipeline
func WithVectorTopN(top_n int) SearchOption {
	return func(search_pipeline []JSON) []JSON {
		if top_n < 0 {
			top_n = _DEFAULT_SEARCH_TOP_N
		}
		search_pipeline[0]["$search"].(JSON)["cosmosSearch"].(JSON)["k"] = top_n
		return search_pipeline
	}
}

// for text searches appending is fine it doesn't impact the other additions of the pipeline
func WithTextTopN(top_n int) SearchOption {
	return func(search_pipeline []JSON) []JSON {
		search_pipeline = append(search_pipeline, JSON{"$limit": top_n})
		return search_pipeline
	}
}

// for both searches appending is fine it doesn't impact the other additions of the pipeline
func WithProjection(fields JSON) SearchOption {
	return func(search_pipeline []JSON) []JSON {
		if len(fields) > 0 {
			search_pipeline = append(search_pipeline, JSON{"$project": fields})
		}
		return search_pipeline
	}
}

// for both searches appending is fine it doesn't impact the other additions of the pipeline
func WithMinSearchScore(score float64) SearchOption {
	return func(search_pipeline []JSON) []JSON {
		search_pipeline = append(search_pipeline,
			JSON{
				"$match": JSON{
					"search_score": JSON{"$gte": score},
				},
			})
		return search_pipeline
	}
}

func createVectorSearchPipeline(query_embeddings []float32, vec_path string, options []SearchOption) []JSON {
	pipeline := createDefaultVectorSearchPipeline(query_embeddings, vec_path)
	for _, opt := range options {
		pipeline = opt(pipeline)
	}
	return pipeline
}

func createTextSearchPipeline(query_texts []string, options []SearchOption) []JSON {
	search_pipeline := createDefaultTextSearchPipeline(query_texts)
	for _, opt := range options {
		search_pipeline = opt(search_pipeline)
	}
	return search_pipeline
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
			"$addFields": JSON{
				"search_score": JSON{"$meta": "searchScore"},
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
			"$addFields": JSON{
				"search_score": JSON{"$meta": "textScore"},
			},
		},
		{
			"$sort": JSON{"search_score": -1},
		},
	}
}
