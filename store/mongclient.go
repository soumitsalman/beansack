package store

import (
	ctx "context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	datautils "github.com/soumitsalman/data-utils"
)

const (
	_DEFAULT_SEARCH_MIN_SCORE = 0.5
	_DEFAULT_SEARCH_TOP_N     = 5
)

type JSON map[string]any

type Store[T any] struct {
	name             string
	collection       *mongo.Collection
	search_min_score float64
	search_top_n     int
}

type Option[T any] func(store *Store[T])

func New[T any](opts ...Option[T]) *Store[T] {
	store := &Store[T]{
		search_min_score: _DEFAULT_SEARCH_MIN_SCORE,
		search_top_n:     _DEFAULT_SEARCH_TOP_N,
	}
	for _, opt := range opts {
		opt(store)
	}
	if store.collection == nil {
		return nil
	}
	return store
}

func WithConnectionString[T any](connection_string, database, collection string) Option[T] {
	return func(store *Store[T]) {
		client := createMongoClient(connection_string)
		if client == nil {
			return
		}
		db := client.Database(database)
		if db == nil {
			return
		}
		col_client := db.Collection(collection)
		if col_client == nil {
			return
		}
		store.name = fmt.Sprintf("%s/%s", database, collection)
		store.collection = col_client
	}
}

func WithMinSearchScore[T any](score float64) Option[T] {
	return func(store *Store[T]) {
		store.search_min_score = score
	}
}

func WithSearchTopN[T any](top_n int) Option[T] {
	return func(store *Store[T]) {
		store.search_top_n = top_n
	}
}

func (store *Store[T]) Add(docs []T) ([]any, error) {
	// this is done for error handling for mongo db
	if len(docs) == 0 {
		log.Printf("[%s]: Empty list of docs, nothing to insert.\n", store.name)
		return nil, nil
	}

	// don't insert if it already exists when there is

	res, err := store.collection.InsertMany(ctx.Background(), datautils.Transform(docs, func(item *T) any { return *item }))
	if err != nil {
		log.Printf("[%s]: Insertion failed. %v\n", store.name, err)
		return nil, err
	}
	log.Printf("[%s]: %d items inserted.\n", store.name, len(res.InsertedIDs))
	return res.InsertedIDs, nil
}

func (store *Store[T]) Update(docs []T, filters []JSON) {
	// create batch
	updates := make([]mongo.WriteModel, len(docs))
	for i := range docs {
		updates[i] = mongo.NewUpdateOneModel().
			SetFilter(filters[i]).
			SetUpdate(JSON{"$set": docs[i]})
	}
	res, err := store.collection.BulkWrite(ctx.Background(), updates)
	if err != nil {
		log.Printf("[%s]: Update failed. %v\v", store.name, err)
		return
	}
	log.Printf("[%s]: %d items updated.\n", store.name, res.MatchedCount)
}

func (store *Store[T]) Get(filter JSON, fields JSON) []T {
	// fields = datautils.AppendMaps(JSON{"_id": 0}, fields)
	return store.extractFromSearchResult(store.collection.Find(ctx.Background(), filter, options.Find().SetProjection(fields)))
}

func (store *Store[T]) Aggregate(pipeline any) []T {
	return store.extractFromSearchResult(store.collection.Aggregate(ctx.Background(), pipeline))
}

// query documents
func (store *Store[T]) Search(query_embeddings []float32, filter JSON, fields JSON) []T {
	cosmos_search := JSON{
		"vector": query_embeddings,
		"path":   "embeddings", // this hardcoded for ease. All embeddings will be in a field called embeddings
		"k":      store.search_top_n,
	}
	if len(filter) > 0 {
		cosmos_search["filter"] = filter
	}
	// js, _ := json.MarshalIndent(cosmos_search, "", "  ")
	// log.Println(string(js))

	fields = datautils.AppendMaps(
		JSON{
			"similarity_score": JSON{"$meta": "searchScore"},
			"_id":              0,
		},
		fields,
	)
	search_pipeline := []JSON{
		{
			"$search": JSON{
				"cosmosSearch":       cosmos_search,
				"returnStoredSource": true,
			},
		},
		{
			"$project": fields,
		},
		{
			"$match": JSON{
				"similarity_score": JSON{"$gte": store.search_min_score},
			},
		},
	}
	return store.Aggregate(search_pipeline)
}

func (store *Store[T]) extractFromSearchResult(cursor *mongo.Cursor, err error) []T {
	background := ctx.Background()
	if err != nil {
		log.Printf("[%s]: Couldn't retrieve items. %v\n", store.name, err)
		return nil
	}
	defer cursor.Close(background)
	var contents []T
	if err = cursor.All(background, &contents); err == nil {
		return contents
	}
	return nil
}

func createMongoClient(connection_string string) *mongo.Client {
	client, err := mongo.Connect(ctx.Background(), options.Client().ApplyURI(connection_string))
	if err != nil {
		log.Println(err)
		return nil
	}
	return client
}
