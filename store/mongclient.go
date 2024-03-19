package store

import (
	ctx "context"
	"fmt"
	"log"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	datautils "github.com/soumitsalman/data-utils"

	"github.com/tmc/langchaingo/embeddings"
	"github.com/tmc/langchaingo/textsplitter"
)

const (
	_EMBEDDINGS = "embeddings"
	_ID         = "_id"
)

type JSON map[string]any

type Store struct {
	name       string
	collection *mongo.Collection
	id_func    func(item any) string
	embedder   embeddings.Embedder
	splitter   textsplitter.TextSplitter
}

type Option func(store *Store)

func New(opts ...Option) *Store {
	store := &Store{}
	for _, opt := range opts {
		opt(store)
	}
	if store.collection == nil {
		return nil
	}
	return store
}

func WithConnectionString(connection_string, database, collection string) Option {
	return func(store *Store) {
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

func WithIdFunction(id_func func(item any) string) Option {
	return func(store *Store) {
		store.id_func = id_func
	}
}

func WithEmbedder(embedder embeddings.Embedder) Option {
	return func(store *Store) {
		store.embedder = embedder
	}
}

func WithTextSplitter(text_splitter textsplitter.TextSplitter) Option {
	return func(store *Store) {
		store.splitter = text_splitter
	}
}

func AddDocuments[T any](store *Store, docs []T) ([]string, error) {
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
	return datautils.Transform[any, string](res.InsertedIDs, func(id *any) string { return (*id).(string) }), nil
}

func GetDocuments[T any](store *Store, filter JSON) []T {
	background := ctx.Background()
	cursor, err := store.collection.Find(background, filter)
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
