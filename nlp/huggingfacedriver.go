package nlp

import (

	// "github.com/tmc/langchaingo/llms/huggingface"
	// "github.com/tmc/langchaingo/vectorstores"

	// llms "github.com/tmc/langchaingo/llms"
	ctx "context"
	"log"
	"os"
	"time"

	"github.com/avast/retry-go"
	hfemb "github.com/tmc/langchaingo/embeddings/huggingface"
	hfllm "github.com/tmc/langchaingo/llms/huggingface"
	"github.com/tmc/langchaingo/textsplitter"
)

const (
	_RETRY_DELAY = 20 * time.Second
)

const (
	_EMBEDDINGS_MODEL = "jinaai/jina-embeddings-v2-small-en"
	_KEYWORDS_MODEL   = "ilsilfverskiold/tech-keywords-extractor"
	_SUMMARY_MODEL    = "google/flan-t5-base"
	_TEXT_CHUNK_SIZE  = 512
)

type HuggingfaceDriver struct {
	// small_embedder *hfemb.Huggingface
	embedder       *hfemb.Huggingface
	text_splitter  textsplitter.TokenSplitter
	keywords_model *hfllm.LLM
	summary_moodel *hfllm.LLM
}

func NewHuggingfaceDriver() *HuggingfaceDriver {
	emb_llm, _ := hfllm.New(hfllm.WithToken(getHuggingfaceToken()))
	embedder, err := hfemb.NewHuggingface(hfemb.WithClient(*emb_llm), hfemb.WithModel(_EMBEDDINGS_MODEL))
	if err != nil {
		log.Printf("[NewHuggingfaceDriver] Failed Loading %s. %v\n", _EMBEDDINGS_MODEL, err)
		return nil
	}
	keywords_model, err := hfllm.New(hfllm.WithToken(getHuggingfaceToken()), hfllm.WithModel(_KEYWORDS_MODEL))
	if err != nil {
		log.Printf("[NewHuggingfaceDriver] Failed Loading %s. %v\n", _KEYWORDS_MODEL, err)
		return nil
	}
	summary_model, err := hfllm.New(hfllm.WithToken(getHuggingfaceToken()), hfllm.WithModel(_SUMMARY_MODEL))
	if err != nil {
		log.Printf("[NewHuggingfaceDriver] Failed Loading %s. %v\n", _SUMMARY_MODEL, err)
		return nil
	}
	return &HuggingfaceDriver{
		text_splitter:  textsplitter.NewTokenSplitter(textsplitter.WithChunkSize(_TEXT_CHUNK_SIZE)),
		embedder:       embedder,
		keywords_model: keywords_model,
		summary_moodel: summary_model,
	}
}

func (driver *HuggingfaceDriver) CreateTextEmbeddings(texts []string) []TextEmbeddings {
	var res []TextEmbeddings
	retry.Do(func() error {
		vecs, err := driver.embedder.EmbedDocuments(ctx.Background(), texts)
		if err != nil {
			log.Printf("[%s]: error generating embeddings.%v\n", _EMBEDDINGS_MODEL, err)
			return err
		}
		res = make([]TextEmbeddings, len(texts))
		for i := range texts {
			res[i].Embeddings = vecs[i]
		}
		return nil
	}, retry.Delay(_RETRY_DELAY))
	return res
}

func getHuggingfaceToken() string {
	return os.Getenv("HUGGINGFACE_API_TOKEN")
}
