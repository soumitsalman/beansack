package sdk

import (
	"os"
	"time"

	"github.com/avast/retry-go"
	"github.com/soumitsalman/beansack/store"
	datautils "github.com/soumitsalman/data-utils"
)

const (
	BEANSACK = "beansack"
	BEANS    = "beans"
	DIGESTS  = "digests"
)

var (
	wholebeans  *store.Store[Bean] = store.New(store.WithConnectionString[Bean](getConnectionString(), BEANSACK, BEANS))
	groundbeans *store.Store[Bean] = store.New(store.WithConnectionString[Bean](getConnectionString(), BEANSACK, DIGESTS))
	nlpdriver   *ParrotBoxDriver   = NewParrotBoxDriver()
)

func AddBeans(beans []Bean) error {
	// assign updated time
	updated_time := time.Now().Unix()
	datautils.ForEach(beans, func(item *Bean) {
		item.Updated = updated_time
	})

	_, err := wholebeans.Add(beans)
	if err != nil {
		return err
	}
	// once the main docs are up, update them with sentiment, summary, keywords and embeddings
	updateNlpAttributes(beans)
	return nil
}

func GetBeans(filter store.JSON) []Bean {
	return wholebeans.Get(filter)
}

func SimilaritySearch(query_text string, filter store.JSON, top_n int, for_rag bool) []Bean {
	embeddings := runRemoteNlpFunction(nlpdriver.CreateTextEmbeddings, []string{query_text})[0].Embeddings
	if for_rag {
		// if this is for content generation bean look for similar smaller stuff in the groundbeans
		return groundbeans.SimilaritySearch(embeddings, filter, top_n)
	} else {
		// or else this is topic similarity. then just look into the whole beans
		return wholebeans.SimilaritySearch(embeddings, filter, top_n)
	}
}

type NlpDriverError string

func (err NlpDriverError) Error() string {
	return string(err)
}

func updateNlpAttributes(beans []Bean) {
	filters := getPointerFilters(beans)
	texts := getTextFields(beans)

	// embeddings
	embs := runRemoteNlpFunction(nlpdriver.CreateBeanEmbeddings, texts)
	wholebeans.Update(embs, filters)

	// summary, keywords, sentiments
	attrs := runRemoteNlpFunction(nlpdriver.CreateBeanAttributes, texts)
	wholebeans.Update(attrs, filters)
}

func runRemoteNlpFunction[T any](nlp_func func(texts []string) ([]T, error), texts []string) []T {
	var res []T
	retry.Do(func() error {
		output, err := nlp_func(texts)
		// something went wrong with the function so try again
		if err != nil || len(output) != len(texts) {
			return NlpDriverError("[dbops] Remote NLP function failed. " + err.Error())
		}
		// generation succeeded
		res = output
		return nil
	}, retry.Delay(_RETRY_DELAY))
	return res
}

func getPointerFilters(beans []Bean) []store.JSON {
	return datautils.Transform(beans, func(bean *Bean) store.JSON {
		return store.JSON{
			"url":     bean.Url,
			"updated": bean.Updated,
		}
	})
}

func getTextFields(beans []Bean) []string {
	return datautils.Transform(beans, func(bean *Bean) string {
		return bean.Text
	})
}

func getConnectionString() string {
	return os.Getenv("DB_CONNECTION_STRING")
}
