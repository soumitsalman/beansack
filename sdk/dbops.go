package sdk

import (
	ctx "context"
	"log"
	"os"

	"github.com/soumitsalman/beansack/store"
	datautils "github.com/soumitsalman/data-utils"

	"github.com/tmc/langchaingo/embeddings"
	hfe "github.com/tmc/langchaingo/embeddings/huggingface"
	hfllm "github.com/tmc/langchaingo/llms/huggingface"
	"github.com/tmc/langchaingo/textsplitter"
)

const (
	BEANSACK = "beansack"
	BEANS    = "beans"
	DIGESTS  = "digests"
)

const (
	_MAX_CHUNK_SIZE   = 512
	_EMBEDDINGS_MODEL = "sentence-transformers/all-mpnet-base-v2"
)

func newEmbedder() embeddings.Embedder {
	hfclient, err := hfllm.New(
		hfllm.WithToken(getHuggingfaceToken()),
		hfllm.WithModel("gpt2"),
	)
	if err != nil {
		log.Fatalln("Failed loading embedder", err)
	}
	embedder, err := hfe.NewHuggingface(
		hfe.WithClient(*hfclient),
		hfe.WithModel(_EMBEDDINGS_MODEL),
	)
	if err != nil {
		log.Fatalln("Failed loading embedder", err)
	}
	return embedder
}

func newTextSplitter() textsplitter.TextSplitter {
	return textsplitter.NewRecursiveCharacter(textsplitter.WithChunkSize(_MAX_CHUNK_SIZE))
}

var (
	beanstore *store.Store = store.New(
		store.WithConnectionString(getConnectionString(), BEANSACK, BEANS),
	)
	digeststore *store.Store = store.New(
		store.WithConnectionString(getConnectionString(), BEANSACK, DIGESTS),
		store.WithTextSplitter(newTextSplitter()),
		store.WithEmbedder(newEmbedder()),
	)
)

func AddBeans(beans []Bean) []string {
	// set the ids
	beans = datautils.ForEach(beans, func(item *Bean) {
		item.Id = item.GetId()
	})
	res, err := store.AddDocuments(beanstore, beans)
	if err != nil {
		return nil
	}
	return res
}

func AddDigest(beans []Bean) []string {
	digest_packs := make([]BeanChunk, 0, len(beans))

	text_splitter := newTextSplitter()
	embedder := newEmbedder()
	datautils.ForEach(beans, func(bean *Bean) {
		chunks, _ := text_splitter.SplitText(bean.Text)
		log.Println(len(chunks), " chunks for", bean.GetId())
		embeddings, err := embedder.EmbedDocuments(ctx.Background(), chunks)
		if err != nil {
			log.Println("Embeeding failed", err)
		} else {
			for i := range chunks {
				digest_packs = append(digest_packs, BeanChunk{BeanId: bean.GetId(), Digest: chunks[i], Embeddings: embeddings[i]})
			}
		}
	})

	res, err := store.AddDocuments(digeststore, digest_packs)
	if err != nil {
		return nil
	}
	return res
}

func GetBeans(filter store.JSON) []Bean {
	return store.GetDocuments[Bean](beanstore, filter)
}

func getConnectionString() string {
	return os.Getenv("DB_CONNECTION_STRING")
}

func getHuggingfaceToken() string {
	return os.Getenv("HUGGINGFACEHUB_API_TOKEN")
}
