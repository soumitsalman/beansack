package internal

import (
	"time"

	"github.com/avast/retry-go"
	"github.com/go-resty/resty/v2"
	datautils "github.com/soumitsalman/data-utils"
	"github.com/tmc/langchaingo/textsplitter"
)

const (
	SHORT_DELAY     = 10 * time.Millisecond
	LONG_DELAY      = 10 * time.Second
	_RETRY_ATTEMPTS = 3
)

func RetryOnFail[T any](original_func func() (T, error), delay time.Duration) T {
	var res T
	var err error
	// retry for each batch
	retry.Do(
		func() error {
			if res, err = original_func(); err != nil {
				// something went wrong with the function so try again
				return err
			}
			// no error
			return nil
		},
		retry.Delay(delay),
		retry.Attempts(_RETRY_ATTEMPTS),
	)
	return res
}

func PostHTTPRequest[T any](url, auth_token string, input any) (T, error) {
	var result T
	req := resty.New().
		SetHeader("Content-Type", "application/json").
		SetHeader("Accept", "application/json").
		R().
		SetBody(input).
		SetResult(&result)
	// if auth token is not empty set it
	if auth_token != "" {
		req = req.SetAuthToken(auth_token)
	}
	// make the request
	_, err := req.Post(url)
	// if there is no error the err value will be `nil`
	return result, err
}

func PostHTTPRequestAndRetryOnFail[T any](url, auth_token string, input any) T {
	var result T
	var err error
	retry.Do(
		func() error {
			result, err = PostHTTPRequest[T](url, auth_token, input)
			// no error
			return err
		},
		retry.Attempts(_RETRY_ATTEMPTS),
		retry.Delay(LONG_DELAY),
	)
	return result
}

func TruncateTextOnTokenCount(texts []string, splitter textsplitter.TokenSplitter) []string {
	return datautils.Transform(texts, func(text *string) string {
		chunks, _ := splitter.SplitText(*text)
		return chunks[0]
	})
}
