package nlp

import (
	"context"
	"fmt"

	datautils "github.com/soumitsalman/data-utils"
	"github.com/tmc/langchaingo/chains"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/prompts"
	"github.com/tmc/langchaingo/schema"
)

const (
	_TEMPLATE = "CONTEXT:\n{{.context}}.\n\n" +
		"OUTPUT FORMAT:\nThe output MUST be in json format wrapped in markdown code format according to the json schema below.\n```json%s```\n\n" + // 1st %s is for schema
		"SAMPLE OUTPUT:\nHere is a sample output format\n```json\n%s\n```\n\n" + // 2nd %s is for `sample value`
		"TASK:\nFor each user input follow the instructions defined in CONTEXT and produce output according to OUTPUT FORMAT.\n\n" +
		"INPUT:\n{{.input_text}}"

	_DEFAULT_OUTPUT_KEY = "value"
)

type JsonValueExtraction struct {
	llm_chain *chains.LLMChain
}

func NewJsonValueExtraction[T any](llm llms.Model, sample_value T) *JsonValueExtraction {
	parser := NewJsonOutputParser[T](sample_value)

	keyconcept_prompt := prompts.NewPromptTemplate(
		fmt.Sprintf(
			_TEMPLATE,
			parser.GetFormatInstructions(),       // output schema
			datautils.ToJsonString(sample_value), // sample output
		),
		[]string{"context", "input_text"},
	)

	internal_chain := chains.NewLLMChain(llm, keyconcept_prompt, chains.WithTemperature(0))
	internal_chain.OutputParser = parser
	internal_chain.OutputKey = _DEFAULT_OUTPUT_KEY

	return &JsonValueExtraction{internal_chain}
}

func (c JsonValueExtraction) Call(ctx context.Context, values map[string]any, options ...chains.ChainCallOption) (map[string]any, error) {
	return c.llm_chain.Call(ctx, values, options...)
}

// GetMemory returns the memory.
func (c JsonValueExtraction) GetMemory() schema.Memory {
	return c.llm_chain.Memory
}

// GetInputKeys returns the expected input keys.
func (c JsonValueExtraction) GetInputKeys() []string {
	return append([]string{}, c.llm_chain.Prompt.GetInputVariables()...)
}

// GetOutputKeys returns the output keys the chain will return.
func (c JsonValueExtraction) GetOutputKeys() []string {
	return []string{c.llm_chain.OutputKey}
}
