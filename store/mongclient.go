package store

import (
	ctx "context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	datautils "github.com/soumitsalman/data-utils"
)

type JSON map[string]any

type Store[T any] struct {
	name       string
	collection *mongo.Collection
}

type Option[T any] func(store *Store[T])

func New[T any](opts ...Option[T]) *Store[T] {
	store := &Store[T]{}
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

func (store *Store[T]) Add(docs []T) ([]any, error) {
	// this is done for error handling for mongo db
	if len(docs) == 0 {
		log.Printf("[%s]: Empty list of docs, nothing to insert.\n", store.name)
		return nil, nil
	}

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
	log.Printf("[%s]: %d items updated.\n", store.name, res.UpsertedCount)
}

func (store *Store[T]) Get(filter JSON) []T {
	background := ctx.Background()
	return store.extractFromSearchResult(store.collection.Find(background, filter))

}

// query documents
func (store *Store[T]) SimilaritySearch(query_embeddings []float32, filter JSON, top_n int) []T {
	cosmos_search := bson.M{
		"vector": query_embeddings,
		"path":   "embeddings", // this hardcoded for ease. All embeddings will be in a field called embeddings
		"k":      top_n,
	}
	if len(filter) > 0 {
		cosmos_search["filter"] = filter
	}
	search_pipeline := mongo.Pipeline{
		{{
			"$search", bson.M{
				"cosmosSearch":       cosmos_search,
				"returnStoredSource": true,
			},
		}},
		{{
			"$project", bson.M{
				"url":     1,
				"updated": 1,
				"text":    1,
			},
		}},
	}
	return store.extractFromSearchResult(store.collection.Aggregate(ctx.Background(), search_pipeline))
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

// func createMediaContentTags(media_content_embeddings []float32) []string {
// 	search_comm := mongo.Pipeline{
// 		{{
// 			"$search", JSON{
// 				"cosmosSearch": JSON{
// 					"vector": query_embeddings,
// 					"path":   "embeddings", // this hardcoded for ease. All embeddings will be in a field called embeddings
// 					"k":      top_n,
// 				}, // return the top item
// 			},
// 		}},
// 		{{
// 			"$project", bson.M{
// 				"_id": 1, //"$$ROOT._id"},
// 			},
// 		}},
// 	}
// 	if cursor, err := getMongoCollection(INTEREST_CATEGORIES).Aggregate(ctx.Background(), search_comm); err != nil {
// 		return nil
// 	} else {
// 		defer cursor.Close(ctx.Background())
// 		var items []CategoryItem
// 		cursor.All(ctx.Background(), &items)
// 		return utils.Transform[CategoryItem, string](items, func(item *CategoryItem) string { return item.Category })
// 	}
// }

func createMongoClient(connection_string string) *mongo.Client {
	client, err := mongo.Connect(ctx.Background(), options.Client().ApplyURI(connection_string))
	if err != nil {
		log.Println(err)
		return nil
	}
	return client
}
