package sdk

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

type ParrotBoxDriver struct {
	url        string
	auth_token string
}

func NewParrotBoxDriver(url string, auth_token string) *ParrotBoxDriver {
	return &ParrotBoxDriver{url, auth_token}
}

func (driver *ParrotBoxDriver) CreateTextEmbeddings(texts []string) ([]TextEmbeddings, error) {
	return postRequest[TextEmbeddings](driver.url, driver.auth_token, _EMBEDDINGS, texts)
}

func (driver *ParrotBoxDriver) CreateBeanEmbeddings(texts []string) ([]Bean, error) {
	return postRequest[Bean](driver.url, driver.auth_token, _EMBEDDINGS, texts)
}

func (driver *ParrotBoxDriver) CreateTextAttributes(texts []string) ([]TextAttributes, error) {
	return postRequest[TextAttributes](driver.url, driver.auth_token, _ATTRIBUTES, texts)
}

func (driver *ParrotBoxDriver) CreateBeanAttributes(texts []string) ([]Bean, error) {
	return postRequest[Bean](driver.url, driver.auth_token, _ATTRIBUTES, texts)
}

func (driver *ParrotBoxDriver) CreateBeanSummary(texts []string) ([]Bean, error) {
	return postRequest[Bean](driver.url, driver.auth_token, _SUMMARY, texts)
}

func (driver *ParrotBoxDriver) CreateBeanKeywords(texts []string) ([]Bean, error) {
	return postRequest[Bean](driver.url, driver.auth_token, _KEYWORDS, texts)
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
