package sdk

import (
	"fmt"
	"log"
	"os"

	"github.com/go-resty/resty/v2"
	datautils "github.com/soumitsalman/data-utils"
)

// var (
// 	_EMBED_URL = os.Getenv("LOCAL_EMBED_URL")
// 	_TOPIC_URL = os.Getenv("LOCAL_TOPIC_URL")
// 	_SUMMARY_URL = os.Getenv("LOCAL_SUMMARY_URL")
// 	_KEYWORDS_URL = os.Getenv("LOCAL_KYEWORDS_URL")

// )

const (
	SEARCH_QUERY    = "search_query"
	SEARCH_DOCUMENT = "search_document"
	CATEGORIZATION  = "classification"
	SIMILARITY      = "clustering"
)

type inferenceInput struct {
	Inputs []string `json:"inputs"`
}

type InferenceDriver struct {
	embed_url string
	topic_url string
}

func NewLocalInferenceDriver() *InferenceDriver {
	return &InferenceDriver{
		embed_url: os.Getenv("EMBED_GENERATION_URL"),
		topic_url: os.Getenv("TOPIC_GENERATION_URL"),
	}
}

func (driver *InferenceDriver) CreateBatchTextEmbeddings(texts []string, task_type string) ([][]float32, error) {
	// prepare input
	inputs := datautils.Transform[string, string](texts, func(item *string) string { return toEmbeddingInput(*item, task_type) })
	// get result
	return sendLocalRequest[[]float32](driver.embed_url, inputs)
}

func (driver *InferenceDriver) CreateTextEmbeddings(text string, task_type string) ([]float32, error) {
	if embs, err := sendLocalRequest[[]float32](driver.embed_url, []string{toEmbeddingInput(text, task_type)}); err != nil {
		return nil, err
	} else {
		return embs[0], nil
	}
}

func toEmbeddingInput(text, task_type string) string {
	if len(task_type) > 0 {
		return fmt.Sprintf("%s: %s", task_type, text)
	}
	return text
}

func sendLocalRequest[T any](url string, inputs []string) ([]T, error) {
	var result []T
	_, err := resty.New().
		SetHeader("Content-Type", "application/json").
		SetHeader("Accept", "application/json").
		R().
		SetBody(inferenceInput{inputs}).
		SetResult(&result).
		Post(url)
	if err != nil {
		log.Printf("[LocalInference| %s] Request Failed. Error: %v\nRetrying ...", url, err)
		return nil, err
	}
	return result, nil
}
