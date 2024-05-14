package embeddings

import (
	"fmt"
	"log"

	"github.com/soumitsalman/beansack/nlp/internal"
	datautils "github.com/soumitsalman/data-utils"
	"github.com/tmc/langchaingo/textsplitter"
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
	_MAX_CHUNK_SIZE = 4096
	_BASE_URL       = "https://embeddings-service.purplesea-08c513a7.eastus.azurecontainerapps.io/embed"
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
	splitter  textsplitter.TokenSplitter
}

func NewEmbeddingsDriver() *EmbeddingsDriver {
	return &EmbeddingsDriver{
		embed_url: _BASE_URL,
		splitter:  textsplitter.NewTokenSplitter(textsplitter.WithChunkSize(_MAX_CHUNK_SIZE), textsplitter.WithChunkOverlap(0)),
	}
}

func (driver *EmbeddingsDriver) CreateBatchTextEmbeddings(texts []string, task_type string) [][]float32 {
	// TODO: automatically figure out input size based on tokens
	input_texts := datautils.Transform(texts, func(item *string) string { return driver.toEmbeddingInput(*item, task_type, false) })
	return driver.createEmbeddings(&inferenceInput{input_texts})
}

func (driver *EmbeddingsDriver) CreateTextEmbeddings(text string, task_type string) []float32 {
	output := driver.createEmbeddings(&inferenceInput{[]string{driver.toEmbeddingInput(text, task_type, true)}})
	if len(output) >= 1 {
		return output[0]
	}
	return nil
}

func (driver *EmbeddingsDriver) toEmbeddingInput(text, task_type string, is_large_text bool) string {
	if is_large_text {
		// split and truncate at the first chunk size
		chunks, _ := driver.splitter.SplitText(text)
		text = chunks[0]
	}
	// prefix with embedding type
	if len(task_type) > 0 {
		text = fmt.Sprintf("%s: %s", task_type, text)
	}
	return text
}

func (driver *EmbeddingsDriver) createEmbeddings(input *inferenceInput) [][]float32 {
	return internal.RetryOnFail(
		func() ([][]float32, error) {
			if embs, err := internal.PostHTTPRequest[[][]float32](driver.embed_url, "", input); err != nil {
				log.Printf("[EmbeddingsDriver] Embedding generation failed. %v\n", err)
				return nil, err
			} else if len(embs) != len(input.Inputs) {
				err_msg := fmt.Sprintf("[EmbeddingsDriver] Embedding generation failed. Expected number of embeddings %d. Generated number of embeddings: %d", len(input.Inputs), len(embs))
				log.Println(err_msg)
				return nil, EmbeddingServerError(err_msg)
			} else {
				return embs, nil
			}
		},
		internal.SHORT_DELAY)
}
