package nlp

import (
	"context"
	"fmt"

	"github.com/tmc/langchaingo/chains"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/prompts"
	"github.com/tmc/langchaingo/schema"
)

const (
	// first %s is for `context` and the second %s is for `format_instructions`
	json_extraction_template = "CONTEXT: %s.\n\n" +
		"OUTPUT FORMAT: %s\n\n" +
		"TASK: Based on the context extract the values in the defined output format from the input content below:\n\n" +
		"{{.input_text}}"

	_default_input_key  = "input_text"
	_default_output_key = "value"
)

type JsonValueExtraction struct {
	llm_chain *chains.LLMChain
}

func NewJsonValueExtraction[T any](llm llms.Model, context string, sample_value T) *JsonValueExtraction {
	parser := NewJsonOutputParser[T](sample_value)

	keyconcept_prompt := prompts.NewPromptTemplate(
		fmt.Sprintf(json_extraction_template, context, parser.GetFormatInstructions()),
		[]string{"input_text"})

	internal_chain := chains.NewLLMChain(llm, keyconcept_prompt, chains.WithTemperature(0))
	internal_chain.OutputParser = parser
	internal_chain.OutputKey = _default_output_key

	return &JsonValueExtraction{internal_chain}
}

func (c JsonValueExtraction) Call(ctx context.Context, values map[string]any, options ...chains.ChainCallOption) (map[string]any, error) {
	return c.llm_chain.Call(ctx, values, options...)
}

// GetMemory returns the memory.
func (c JsonValueExtraction) GetMemory() schema.Memory { //nolint:ireturn
	return c.llm_chain.Memory //nolint:ireturn
}

// GetInputKeys returns the expected input keys.
func (c JsonValueExtraction) GetInputKeys() []string {
	return append([]string{}, c.llm_chain.Prompt.GetInputVariables()...)
}

// GetOutputKeys returns the output keys the chain will return.
func (c JsonValueExtraction) GetOutputKeys() []string {
	return []string{c.llm_chain.OutputKey}
}
