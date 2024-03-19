package sdk

import (
	"os"
	"time"

	"github.com/soumitsalman/beansack/store"
	datautils "github.com/soumitsalman/data-utils"
)

const (
	BEANSACK = "beansack"
	BEANS    = "beans"
	DIGESTS  = "digests"
)

var (
	beanstore      *store.Store[Bean]           = store.New(store.WithConnectionString[Bean](getConnectionString(), BEANSACK, BEANS))
	embeddingstore *store.Store[BeanEmbeddings] = store.New(store.WithConnectionString[BeanEmbeddings](getConnectionString(), BEANSACK, DIGESTS))
)

func AddBeans(beans []Bean) error {
	// assign updated time
	updated_time := time.Now().Unix()
	datautils.ForEach(beans, func(item *Bean) {
		item.Updated = updated_time
	})

	_, err := beanstore.AddDocuments(beans)
	if err != nil {
		return err
	}
	// once the main docs are up, update them with sentiment, summary, keywords and embeddings
	updateNlpAttributes(beans)
	return nil
}

func updateNlpAttributes(beans []Bean) {
	embs := CreateBeanEmbeddings(beans)
	embeddingstore.AddDocuments(embs)

	attrs := CreateAttributes(beans)
	beanstore.UpdateDocuments(attrs, createPointerFilters(beans))
}

func createPointerFilters(beans []Bean) []store.JSON {
	return datautils.Transform[Bean, store.JSON](beans, func(bean *Bean) store.JSON {
		return store.JSON{
			"url":     bean.Url,
			"updated": bean.Updated,
		}
	})
}

func GetBeans(filter store.JSON) []Bean {
	return beanstore.GetDocuments(filter)
}

func getConnectionString() string {
	return os.Getenv("DB_CONNECTION_STRING")
}
