package nlp

import (
	"fmt"
	"log"

	datautils "github.com/soumitsalman/data-utils"
)

const (
	SEARCH_QUERY    = "search_query"
	SEARCH_DOCUMENT = "search_document"
	CATEGORIZATION  = "classification"
	SIMILARITY      = "clustering"
)

const (
	// 4096 tokens is roughly 4/5 pages and is around 15 to 20 minutes reading time for a news or post
	// Although the current embeddings-service model can take 8192, there is no functional reason to ingest that much content
	// _MAX_CHUNK_SIZE = 4096
	_EMBEDDER_BASE_URL = "https://embeddings-service.purplesea-08c513a7.eastus.azurecontainerapps.io/embed"
	_EMBEDDER_WINDOW   = 8191
)

type inferenceInput struct {
	Inputs []string `json:"inputs"`
}

type EmbeddingServerError string

func (err EmbeddingServerError) Error() string {
	return string(err)
}

type EmbeddingsDriver struct {
	embed_url string
	// splitter  textsplitter.TokenSplitter
}

func NewEmbeddingsDriver(base_url string) *EmbeddingsDriver {
	driver := &EmbeddingsDriver{
		embed_url: _EMBEDDER_BASE_URL,
	}
	if len(base_url) > 0 {
		driver.embed_url = base_url
	}
	return driver
}

func (driver *EmbeddingsDriver) CreateBatchTextEmbeddings(texts []string, task_type string) [][]float32 {
	// if the count is over the window size split in half and try
	if CountTokens(texts) > _EMBEDDER_WINDOW {
		return append(
			driver.CreateBatchTextEmbeddings(texts[:len(texts)/2], task_type),
			driver.CreateBatchTextEmbeddings(texts[len(texts)/2:], task_type)...)
	}
	input_texts := datautils.Transform(texts, func(item *string) string { return driver.toEmbeddingInput(*item, task_type) })
	embs := driver.createEmbeddings(&inferenceInput{input_texts})
	// if the embeddings generation is failing insert duds
	if embs == nil {
		return make([][]float32, len(texts))
	}
	return embs
}

func (driver *EmbeddingsDriver) CreateTextEmbeddings(text string, task_type string) []float32 {
	output := driver.createEmbeddings(&inferenceInput{[]string{driver.toEmbeddingInput(text, task_type)}})
	if len(output) >= 1 {
		return output[0]
	}
	return nil
}

func (driver *EmbeddingsDriver) toEmbeddingInput(text, task_type string) string {
	if len(task_type) > 0 {
		text = fmt.Sprintf("%s: %s", task_type, text)
	}
	return text
}

func (driver *EmbeddingsDriver) createEmbeddings(input *inferenceInput) [][]float32 {
	return retryT(
		func() ([][]float32, error) {
			if embs, err := postHTTPRequest[[][]float32](driver.embed_url, "", input); err != nil {
				log.Printf("[EmbeddingsDriver] Embedding generation failed. %v\n", err)
				return nil, err
			} else if len(embs) != len(input.Inputs) {
				err_msg := fmt.Sprintf("[EmbeddingsDriver] Embedding generation failed. Expected number of embeddings %d. Generated number of embeddings: %d", len(input.Inputs), len(embs))
				log.Println(err_msg)
				return nil, EmbeddingServerError(err_msg)
			} else {
				return embs, nil
			}
		})
}
