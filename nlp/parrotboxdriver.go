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
	_MODEL        = "llama3-8b-8192"
	_BASE_URL     = "https://api.groq.com/openai/v1"
	_MODEL_WINDOW = 6144 // reducing the size to account for instructions and samples
)

const (
	_BATCH_DELIMETER = "\n```\n"
)

type ParrotboxClient struct {
	concepts_chain *JsonValueExtraction
	digest_chain   *JsonValueExtraction
}

func NewParrotboxClient(api_key string) *ParrotboxClient {
	client, err := openai.New(
		openai.WithBaseURL(_BASE_URL),
		openai.WithModel(_MODEL),
		openai.WithToken(api_key),
		openai.WithResponseFormat(openai.ResponseFormatJSON))

	if err != nil {
		log.Println(err)
		return nil
	}
	return &ParrotboxClient{
		concepts_chain: NewJsonValueExtraction(client, _CONCEPTS_SAMPLE_OUTPUT),
		digest_chain:   NewJsonValueExtraction(client, _DIGEST_SAMPLE_OUTPUT),
	}
}

func (client *ParrotboxClient) ExtractDigests(texts []string) []Digest {
	output := make([]Digest, 0, len(texts))
	datautils.ForEach(texts, func(text *string) {
		res := serverErrorRetry(
			func() (Digest, error) {
				result, err := client.digest_chain.Call(
					ctx.Background(),
					map[string]any{
						"context":    _DIGEST_INSTRUCTION,
						"input_text": text,
					},
				)
				if err != nil {
					result, err = retryIfParseError(client.digest_chain, err)
				}
				// now check if there is an error. If there is server error the serverErrorRetry will try again
				if err != nil {
					log.Println("[goparrotboxdriver] ExtractDigest failed.", err)
					// insert duds for this batch.
					return Digest{}, err // inserting dud
				}
				return result["value"].(Digest), nil
			})
		output = append(output, res)
	})
	return output
}

func (client *ParrotboxClient) ExtractKeyConcepts(texts []string) []KeyConcept {
	output := make([]KeyConcept, 0, len(texts))
	datautils.ForEach(stuffAndBatchInput(texts), func(batch *string) {
		// retry for each batch
		// if a batch doesnt workout, just move on to the next batch. No need to insert duds since no sequence need to be maintained
		res := serverErrorRetry(
			func() ([]KeyConcept, error) {
				result, err := client.concepts_chain.Call(
					ctx.Background(),
					map[string]any{
						"context":    _CONCEPTS_INSTRUCTION,
						"input_text": batch,
					},
				)
				if err != nil {
					result, err = retryIfParseError(client.concepts_chain, err)
				}
				// now check if there is an error. If there is server error the serverErrorRetry will try again
				if err != nil {
					log.Println("[goparrotboxdriver] ExtractKeyConcepts failed.")
					// insert duds for this batch.
					return nil, err
				}
				return result["value"].([]KeyConcept), nil
			})
		if len(res) > 0 {
			output = append(output, res...)
		}
	})
	return output
}

// the error can be a parse error because content isn't json, or it can be server error
// for server error try again multiple times
// for parser error try with the _RETRY_INSTRUCTION
func retryIfParseError(chain *JsonValueExtraction, err error) (map[string]any, error) {
	var result map[string]any
	log.Println("[parrotboxdiver]", err)
	// try once and send whatever the result is
	if parse_err, ok := err.(ParseError); ok {
		// this is ParserError. retry once
		log.Println("[goparrotboxdriver] Retyring json format extraction.")
		// reassigning the result and err
		result, err = chain.Call(
			ctx.Background(),
			map[string]any{
				"context":    _RETRY_INSTRUCTION,
				"input_text": parse_err.Text,
			},
		)
	}
	// send whatever is there
	return result, err
}

func stuffAndBatchInput(texts []string) []string {
	if CountTokens(texts) > _MODEL_WINDOW {
		// split in half and retry recursively
		return append(
			stuffAndBatchInput(texts[:len(texts)/2]),
			stuffAndBatchInput(texts[len(texts)/2:])...)
	}
	// it is within context window so just batch em up all together
	return []string{fmt.Sprintf("```\n%s\n```", strings.Join(texts, _BATCH_DELIMETER))}
}
