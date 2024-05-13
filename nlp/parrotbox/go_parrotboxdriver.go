package parrotbox

import (
	ctx "context"
	"fmt"
	"log"
	"strings"

	"github.com/soumitsalman/beansack/nlp/internal"
	datautils "github.com/soumitsalman/data-utils"
	"github.com/tmc/langchaingo/llms/openai"
	"github.com/tmc/langchaingo/textsplitter"
)

const (
	_MODEL          = "llama3-8b-8192"
	_BASE_URL       = "https://api.groq.com/openai/v1"
	_MAX_CHUNK_SIZE = 2048
)

const (
	_DIGEST_EXTRACTION_INSTRUCTION = "You are provided with one documents delimitered by ```\n" +
		"Your task is to extract the main digest of the document.\n" +
		"You MUST return exactly one digest.\n" +
		"A 'digest' contains a concise summary of the content and the content topic."
	_CONCEPTS_EXTRACTION_INSTRUCTION = "You are provided with one or more documents delimitered by ```\n" +
		"Your task is to extract the main keyconcepts from each document.\n" +
		"Each document can have more than one keyconcepts. Your output will be a list of keyconcepts.\n" +
		"A 'keyconcept' is one of the main messages or information that is central to the a news article, document or social media post.\n" +
		"A 'keyconcept' has a 'keyphrase' and an associated 'event' and 'description'."
	_BATCH_DELIMETER = "\n```\n"
	_BATCH_SIZE      = 3 // this is because the documents do not exceed 2048 tokens and the context window for the _MODEL is 8192
)

var (
	_digest_sample = Digest{
		Summary: "Sabreen Jouda, a premature baby, was born after an Israeli airstrike killed her mother, father, and 4-year-old sister in Gaza's Rafah city. The baby was delivered by emergency cesarean section at a Kuwaiti hospital and is currently in intensive care. Her paternal grandmother has taken on the responsibility of caring for her. The airstrike also killed 17 children and two women from an extended family. The war in Gaza has resulted in the deaths of at least two-thirds of the over 34,000 Palestinians killed, with children and women being the majority of the victims.",
		Topic:   "Israel Hamas War",
	}

	_concepts_sample = []KeyConcept{
		{
			KeyPhrase:   "ByteDance",
			Event:       "Mounting a court challenge in the United States",
			Description: "ByteDance, the Chinese-owned platform, has announced it will mount a court challenge in the United States to an \"unconstitutional\" law making its way through Congress that could require the platform to be sold or banned in that country.",
		},
		{
			KeyPhrase:   "Adobe Commerce and Magento",
			Event:       "Releasing security patches",
			Description: "Adobe resolved the security bug in February and released security patches for Adobe Commerce and Magento, urging e-tailers to upgrade to 2.4.6-p4, 2.4.5-p6, or 2.4.4-p7 to be protected from the threat.",
		},
	}
)

type GoParrotboxClient struct {
	concept_chain *JsonValueExtraction
	digest_chain  *JsonValueExtraction
	splitter      textsplitter.TokenSplitter
}

func NewGoParrotboxClient(api_key string) *GoParrotboxClient {
	client, err := openai.New(
		openai.WithBaseURL(_BASE_URL),
		openai.WithModel(_MODEL),
		openai.WithToken(api_key),
		openai.WithResponseFormat(openai.ResponseFormatJSON))

	if err != nil {
		log.Println(err)
		return nil
	}
	return &GoParrotboxClient{
		concept_chain: NewJsonValueExtraction(client, _CONCEPTS_EXTRACTION_INSTRUCTION, _concepts_sample),
		digest_chain:  NewJsonValueExtraction(client, _DIGEST_EXTRACTION_INSTRUCTION, _digest_sample),
		splitter:      textsplitter.NewTokenSplitter(textsplitter.WithChunkSize(_MAX_CHUNK_SIZE), textsplitter.WithChunkOverlap(0)),
	}
}

// sequence of which matters in this since it will map to the corresponding text
// lesson learnt: batching a couple of texts together is non-deterministic
//
//	since the language model will sometimes return 1 or 2 items in the array instead of _BATCH_SIZE
//	so send 1 text at a time
func (client *GoParrotboxClient) ExtractDigests(texts []string) []Digest {
	output := make([]Digest, 0, len(texts))
	datautils.ForEach(texts, func(text *string) {
		// retry for item. if it doesnt workout, insert dud so that the sequence is maintained and move on to the next item
		res := internal.RetryOnFail(
			func() (Digest, error) {
				result, err := client.digest_chain.Call(ctx.Background(), map[string]any{"input_text": *text})
				if err != nil {
					log.Println("[goparrotboxdriver] ExtractDigest failed.", err)
					// insert dud
					return Digest{}, err
				}
				// log.Printf("[goparrotboxdriver | DEBUG ONLY] ExtractDigest succeed.")
				return result["value"].(Digest), nil
			})
		output = append(output, res)
	})
	return output
}

func (client *GoParrotboxClient) ExtractKeyConcepts(texts []string) []KeyConcept {
	output := make([]KeyConcept, 0, len(texts))
	batches := client.stuffAndBatchInput(texts)
	datautils.ForEach(batches, func(batch *string) {
		// retry for each batch
		// if a batch doesnt workout, just move on to the next batch. No need to insert duds since no sequence need to be maintained
		res := internal.RetryOnFail(
			func() ([]KeyConcept, error) {
				result, err := client.concept_chain.Call(ctx.Background(), map[string]any{"input_text": batch})
				if err != nil {
					log.Println("[goparrotboxdriver] ExtractKeyConcepts failed.", err)
					return nil, err
				}
				// log.Printf("[goparrotboxdriver | DEBUG ONLY] ExtractKeyConcepts succeed. %d\n", len(result["value"].([]KeyConcept)))
				return result["value"].([]KeyConcept), nil
			})
		if len(res) > 0 {
			output = append(output, res...)
		}
	})
	return output
}

func (client *GoParrotboxClient) stuffAndBatchInput(texts []string) []string {
	output := make([]string, 0, 1+(len(texts)/_BATCH_SIZE))
	// truncate to _MAX_CHUNK_SIZE tokens
	texts = internal.TruncateTextOnTokenCount(texts, client.splitter)
	// a batch with truncated texts
	for i := 0; i < len(texts); i += _BATCH_SIZE {
		output = append(output, fmt.Sprintf("```\n%s\n```", strings.Join(datautils.SafeSlice(texts, i, i+_BATCH_SIZE), _BATCH_DELIMETER)))
	}
	return output
}
