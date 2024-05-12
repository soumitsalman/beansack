package nlp

import (
	ctx "context"
	"fmt"
	"log"
	"strings"

	datautils "github.com/soumitsalman/data-utils"
	"github.com/tmc/langchaingo/llms/openai"
)

const (
	_MODEL    = "llama3-8b-8192"
	_BASE_URL = "https://api.groq.com/openai/v1"
	// _TOKEN    = "LLMSERVICE_API_KEY"
)

const (
	_DIGEST_EXTRACTION_INSTRUCTION = "You are provided with one or more documents delimitered by ```\n" +
		"Your task is to extract the main digest from each document.\n" +
		"Each document MUST HAVE exactly one digest. Your output will be a list of digests.\n" +
		"A digest contains a concise summary of the content and the content topic."
	_CONCEPTS_EXTRACTION_INSTRUCTION = "You are provided with one or more documents delimitered by ```\n" +
		"Your task is to extract the main keyconcepts from each document.\n" +
		"Each document can have more than one keyconcepts. Your output will be a list of keyconcepts.\n" +
		"A 'keyconcept' is one of the main messages or information that is central to the a news article, document or social media post.\n" +
		"A 'keyconcept' has a 'keyphrase' and an associated 'event' and 'description'."
	_BATCH_DELIMETER = "\n```\n"
	_BATCH_SIZE      = 3 // this is because the documents do not exceed 2048 tokens and the context window for the _MODEL is 8192
)

var (
	_digest_sample = []Digest{
		{
			Summary: "Sabreen Jouda, a premature baby, was born after an Israeli airstrike killed her mother, father, and 4-year-old sister in Gaza's Rafah city. The baby was delivered by emergency cesarean section at a Kuwaiti hospital and is currently in intensive care. Her paternal grandmother has taken on the responsibility of caring for her. The airstrike also killed 17 children and two women from an extended family. The war in Gaza has resulted in the deaths of at least two-thirds of the over 34,000 Palestinians killed, with children and women being the majority of the victims.",
			Topic:   "Israel Hamas War",
		},
	}

	_concepts_sample = []KeyConcept{
		{
			KeyPhrase:   "Stripe payment skimmer",
			Event:       "Capturing and exfiltrating payment data",
			Description: "Magecart attackers are using a Stripe payment skimmer to capture and exfiltrate payment data to an attacker-controlled site.",
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
	}
}

// TODO: stuff texts and create batch

func (client GoParrotboxClient) ExtractDigests(texts []string) ([]Digest, error) {
	output := make([]Digest, 0, len(texts))
	batches := stuffAndBatchInput(texts)
	for _, batch := range batches {
		result, err := client.digest_chain.Call(ctx.Background(), map[string]any{"input_text": batch})
		if err != nil {
			log.Println("[goparrotboxdriver] ExtractDigest failed", err)
			return nil, err
		} else {
			output = append(output, result["value"].([]Digest)...)
		}
	}
	return output, nil
}

func (client GoParrotboxClient) ExtractKeyConcepts(texts []string) ([]KeyConcept, error) {
	output := make([]KeyConcept, 0, len(texts))
	batches := stuffAndBatchInput(texts)
	for _, batch := range batches {
		result, err := client.concept_chain.Call(ctx.Background(), map[string]any{"input_text": batch})
		if err != nil {
			log.Println("[goparrotboxdriver] ExtractKeyConcepts failed", err)
			return nil, err
		} else {
			output = append(output, result["value"].([]KeyConcept)...)
		}
	}
	return output, nil

}

func stuffAndBatchInput(texts []string) []string {
	output := make([]string, 0, 1+(len(texts)/_BATCH_SIZE))
	for i := 0; i < len(texts); i += _BATCH_SIZE {
		output = append(output, fmt.Sprintf("```\n%s\n```", strings.Join(datautils.SafeSlice(texts, i, i+_BATCH_SIZE), _BATCH_DELIMETER)))
	}
	return output
}
