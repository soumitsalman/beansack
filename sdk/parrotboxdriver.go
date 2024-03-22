package sdk

import (
	"log"
	"os"

	"github.com/go-resty/resty/v2"
)

const (
	_EMBEDDINGS = "/text/embeddings"
	_ATTRIBUTES = "/text/attributes"
	_SUMMARY    = "/text/summary"
	_KEYWORDS   = "/text/keywords"
)

type ParrotBoxDriver struct {
	url string
}

func NewParrotBoxDriver() *ParrotBoxDriver {
	return &ParrotBoxDriver{getParrotBoxUrl()}
}

// func (driver *ParrotBoxDriver) CreateTextEmbeddings_v2(texts []string) []map[string]any {
// 	return postRequest[map[string]any](driver.url, _EMBEDDINGS, texts)
// }

func (driver *ParrotBoxDriver) CreateTextEmbeddings(texts []string) ([]TextEmbeddings, error) {
	return postRequest[TextEmbeddings](driver.url, _EMBEDDINGS, texts)
}

func (driver *ParrotBoxDriver) CreateBeanEmbeddings(texts []string) ([]Bean, error) {
	return postRequest[Bean](driver.url, _EMBEDDINGS, texts)
}

func (driver *ParrotBoxDriver) CreateTextAttributes(texts []string) ([]TextAttributes, error) {
	return postRequest[TextAttributes](driver.url, _ATTRIBUTES, texts)
}

func (driver *ParrotBoxDriver) CreateBeanAttributes(texts []string) ([]Bean, error) {
	return postRequest[Bean](driver.url, _ATTRIBUTES, texts)
}

func (driver *ParrotBoxDriver) CreateBeanSummary(texts []string) ([]Bean, error) {
	return postRequest[Bean](driver.url, _SUMMARY, texts)
}

func (driver *ParrotBoxDriver) CreateBeanKeywords(texts []string) ([]Bean, error) {
	return postRequest[Bean](driver.url, _KEYWORDS, texts)
}

func postRequest[T any](url, endpoint string, body any) ([]T, error) {
	var result []T
	_, err := resty.New().
		SetBaseURL(url).
		SetHeader("Content-Type", "application/json").
		SetHeader("Accept", "application/json").
		R().
		SetBody(body).
		SetResult(&result).
		Post(endpoint)
	if err != nil {
		log.Printf("[ParrotBox%s] Request Failed. Error: %v\nRetrying ...", endpoint, err)
		return nil, err
	}
	return result, nil
}

func getParrotBoxUrl() string {
	return os.Getenv("PARROTBOX_URL")
}
