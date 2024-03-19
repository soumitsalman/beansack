package sdk

import (
	"os"

	"github.com/go-resty/resty/v2"
	datautils "github.com/soumitsalman/data-utils"
)

const (
	_EMBEDDINGS = "/embeddings"
	_ATTRIBUTES = "/attributes"
)

func CreateTextEmbeddings(texts []string) []TextEmbeddings {
	var result []struct {
		EmbeddingsList []TextEmbeddings `json:"embeddings,omitempty"`
	}
	if _, err := newParrotBoxRequest().
		SetBody(texts).
		SetResult(&result).
		Post(_EMBEDDINGS); err != nil {
		return nil
	}

	combined_result := make([]TextEmbeddings, 0, len(texts))
	for i := range texts {
		combined_result = append(combined_result, result[i].EmbeddingsList...)
	}
	return combined_result
}

func CreateBeanEmbeddings(beans []Bean) []BeanEmbeddings {
	var result []struct {
		EmbeddingsList []BeanEmbeddings `json:"embeddings,omitempty"`
	}
	if _, err := newParrotBoxRequest().
		SetBody(datautils.Transform(beans, func(item *Bean) string { return item.Text })).
		SetResult(&result).
		Post(_EMBEDDINGS); err != nil {
		return nil
	}

	combined_result := make([]BeanEmbeddings, 0, len(beans))
	for i := range beans {
		datautils.ForEach(result[i].EmbeddingsList, func(item *BeanEmbeddings) {
			// these 2 create the pointer to the original bean
			item.BeanUrl = beans[i].Url
			item.Updated = beans[i].Updated
		})
		combined_result = append(combined_result, result[i].EmbeddingsList...)
	}
	return combined_result
}

func CreateAttributes(beans []Bean) []Bean {
	var result []Bean
	if _, err := newParrotBoxRequest().
		SetBody(datautils.Transform(beans, func(item *Bean) string { return item.Text })).
		SetResult(&result).
		Post(_ATTRIBUTES); err != nil {
		return nil
	}

	// point to the right URL
	for i := range beans {
		// these 2 create the unique pointer to the original bean
		result[i].Url = beans[i].Url
		result[i].Updated = beans[i].Updated
		// add the list the of keywords
		result[i].Keywords = append(result[i].Keywords, beans[i].Keywords...)
	}
	return result
}

func newParrotBoxRequest() *resty.Request {
	return resty.New().
		SetBaseURL(getParrotBoxUrl()).
		SetHeader("Content-Type", "application/json").
		SetHeader("Accept", "application/json").
		R()
}

func getParrotBoxUrl() string {
	return os.Getenv("PARROTBOX_URL")
}
