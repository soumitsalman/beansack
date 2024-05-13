package parrotbox

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/invopop/jsonschema"
	datautils "github.com/soumitsalman/data-utils"
	"github.com/tmc/langchaingo/llms"
	"github.com/tmc/langchaingo/outputparser"
)

const (
	_structuredFormatInstructionTemplate = "The output MUST be a markdown code snippet formatted in the following JSON schema: \n" +
		"```json\n%s\n```\n\n" +
		"An example response would be\n```json\n%s\n```"
)

// This is an expansion of https://github.com/tmc/langchaingo/outputparsers/Structured
// This can take more generic structured types such as arrays and fields with nested json values
type JsonOutputParser[T any] struct {
	data_schema   *jsonschema.Schema
	example_value T
}

// This takes an input value as a sample value
// It includes the json schema of the type T and the sample value itself as part of the format instruction to increase chances of accuracy
func NewJsonOutputParser[T any](sample_val T) JsonOutputParser[T] {
	return JsonOutputParser[T]{
		data_schema:   jsonschema.Reflect(sample_val),
		example_value: sample_val,
	}
}

// Parse parses the output of an LLM into a map. If the content fails to serialize it will return an error
// or else it will return a value of type T
func (p JsonOutputParser[T]) ParseT(text string) (T, error) {
	var parsed T

	// Remove the ```json that should be at the start of the text, and the ```
	// that should be at the end of the text.
	withoutJSONStart := strings.Split(text, "```json")
	if !(len(withoutJSONStart) > 1) {
		return parsed, outputparser.ParseError{Text: text, Reason: "no ```json at start of output"}
	}

	withoutJSONEnd := strings.Split(withoutJSONStart[1], "```")
	if len(withoutJSONEnd) < 1 {
		return parsed, outputparser.ParseError{Text: text, Reason: "no ``` at end of output"}
	}

	jsonString := withoutJSONEnd[0]

	err := json.Unmarshal([]byte(jsonString), &parsed)
	if err != nil {
		log.Printf("[%s] Failed unmarshalling for %s. %v", p.Type(), jsonString, err)
		return parsed, err
	}

	return parsed, nil
}

func (p JsonOutputParser[T]) Parse(text string) (any, error) {
	return p.ParseT(text)
}

// ParseWithPrompt does the same as Parse.
func (p JsonOutputParser[T]) ParseWithPrompt(text string, _ llms.PromptValue) (any, error) {
	return p.ParseT(text)
}

// GetFormatInstructions returns a string explaining how the llm should format
// its response.
func (p JsonOutputParser[T]) GetFormatInstructions() string {
	return fmt.Sprintf(_structuredFormatInstructionTemplate, datautils.ToJsonString(p.data_schema), datautils.ToJsonString(p.example_value))
}

// Type returns the type of the output parser.
func (p JsonOutputParser[T]) Type() string {
	return "json_output_parser_" + reflect.TypeOf(p.example_value).Name()
}