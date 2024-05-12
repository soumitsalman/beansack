package nlp

import (
	"log"

	"github.com/go-resty/resty/v2"
)

const (
	_EMBEDDINGS = "/text/embeddings"
	_ATTRIBUTES = "/text/attributes"
	_SUMMARY    = "/text/summary"
	_KEYWORDS   = "/text/keywords"
)

type PyParrotBoxDriver struct {
	url        string
	auth_token string
}

func NewParrotBoxDriver(url string, auth_token string) *PyParrotBoxDriver {
	return &PyParrotBoxDriver{url, auth_token}
}

func (driver *PyParrotBoxDriver) CreateTextEmbeddings(texts []string) ([]TextEmbeddings, error) {
	return postRequest[TextEmbeddings](driver.url, driver.auth_token, _EMBEDDINGS, texts)
}

func (driver *PyParrotBoxDriver) CreateTextAttributes(texts []string) ([]Digest, error) {
	return postRequest[Digest](driver.url, driver.auth_token, _ATTRIBUTES, texts)
}

func (driver *PyParrotBoxDriver) CreateTextSummary(texts []string) ([]Digest, error) {
	return postRequest[Digest](driver.url, driver.auth_token, _SUMMARY, texts)
}

func (driver *PyParrotBoxDriver) CreateTextKeywords(texts []string) ([]Digest, error) {
	return postRequest[Digest](driver.url, driver.auth_token, _KEYWORDS, texts)
}

func postRequest[T any](url, auth_token, endpoint string, body any) ([]T, error) {
	var result []T
	_, err := resty.New().
		SetBaseURL(url).
		SetHeader("Content-Type", "application/json").
		SetHeader("Accept", "application/json").
		SetHeader("X-API-Key", auth_token).
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
