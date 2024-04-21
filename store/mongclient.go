package store

import (
	ctx "context"
	"fmt"
	"log"
	"strings"

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
	get_id           func(data *T) JSON
	equals           func(a, b *T) bool
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

func WithDataIDAndEqualsFunction[T any](id_func func(data *T) JSON, equals func(a, b *T) bool) Option[T] {
	return func(store *Store[T]) {
		store.get_id = id_func
		store.equals = equals
	}
}

func (store *Store[T]) Add(docs []T) ([]T, error) {
	// this is done for error handling for mongo db
	if len(docs) == 0 {
		log.Printf("[%s]: Empty list of docs, nothing to insert.\n", store.name)
		return nil, nil
	}

	// don't insert if it already exists
	// if there is no id function then treat each item as unique
	if store.get_id != nil && store.equals != nil {
		existing_items := store.Get(JSON{"$or": store.getIDs(docs)}, nil)
		docs = datautils.Filter(docs, func(item *T) bool {
			return !datautils.In(*item, existing_items, store.equals)
		})
	}

	res, err := store.collection.InsertMany(ctx.Background(), datautils.Transform(docs, func(item *T) any { return *item }))
	if err != nil {
		log.Printf("[%s]: Insertion failed. %v\n", store.name, err)
		return nil, err
	}
	log.Printf("[%s]: %d items inserted.\n", store.name, len(res.InsertedIDs))
	return docs, nil
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
	return store.extractFromCursor(store.collection.Find(ctx.Background(), filter, options.Find().SetProjection(fields)))
}

func (store *Store[T]) Aggregate(pipeline any) []T {
	return store.extractFromCursor(store.collection.Aggregate(ctx.Background(), pipeline))
}

func (store *Store[T]) TextSearch(query_texts []string, filter JSON, fields JSON) []T {
	search_pipeline := []JSON{
		{
			"$match": datautils.AppendMaps(
				JSON{
					"$text": JSON{"$search": strings.Join(query_texts, ", ")},
				},
				filter),
		},
		{
			"$project": datautils.AppendMaps(
				JSON{
					"_id":          0,
					"search_score": JSON{"$meta": "textScore"},
				},
				fields),
		},
		{
			"$match": JSON{
				"search_score": JSON{"$gt": 1},
			},
		},
		{
			"$sort": JSON{"search_score": -1},
		},
	}
	return store.Aggregate(search_pipeline)
}

// query documents
func (store *Store[T]) VectorSearch(query_embeddings []float32, vec_path string, filter JSON, fields JSON) []T {
	cosmos_search := JSON{
		"vector": query_embeddings,
		"path":   vec_path, // this hardcoded for ease. All embeddings will be in a field called embeddings
		"k":      store.search_top_n,
	}
	if len(filter) > 0 {
		cosmos_search["filter"] = filter
	}
	// js, _ := json.MarshalIndent(cosmos_search, "", "  ")
	// log.Println(datautils.ToJsonString(cosmos_search))

	fields = datautils.AppendMaps(
		JSON{
			"search_score": JSON{"$meta": "searchScore"},
			"_id":          0,
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
				"search_score": JSON{"$gte": store.search_min_score},
			},
		},
	}
	return store.Aggregate(search_pipeline)
}

func (store *Store[T]) Delete(filter JSON) {
	res, err := store.collection.DeleteMany(ctx.Background(), filter)
	if err != nil {
		log.Printf("[%s]: Deletion failed. %v\n", store.name, err)
	} else {
		log.Printf("[%s]: %d items deleted.\n", store.name, res.DeletedCount)
	}
}

func (store *Store[T]) getIDs(items []T) []JSON {
	return datautils.Transform(items, func(item *T) JSON {
		return store.get_id(item)
	})
}

func (store *Store[T]) extractFromCursor(cursor *mongo.Cursor, err error) []T {
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
		log.Println("[mongoclient]", err)
		return nil
	}
	return client
}
