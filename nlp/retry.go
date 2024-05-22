package nlp

import (
	"regexp"
	"time"

	"github.com/avast/retry-go"
	"github.com/go-resty/resty/v2"
)

const (
	SHORT_DELAY    = 10 * time.Millisecond
	LONG_DELAY     = 10 * time.Second
	RETRY_ATTEMPTS = 3
)

func serverErrorRetry[T any](original_func func() (T, error)) T {
	var res T
	var err error

	retry.Do(
		func() error {
			if res, err = original_func(); err != nil {
				// something went wrong with the function so try again
				return err
			}
			// no error
			return nil
		},
		retry.Delay(LONG_DELAY),
		retry.Attempts(RETRY_ATTEMPTS),
		retry.RetryIf(func(err error) bool {
			// match for 503: Service Unavailable & 429: Rate limit
			res, err := regexp.MatchString("(?i)(429:.+Rate.+limit|503: Service Unavailable)", err.Error())
			return err == nil && res
		}),
	)
	return res
}

func retryT[T any](original_func func() (T, error)) T {
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
		retry.Delay(SHORT_DELAY),
		retry.Attempts(RETRY_ATTEMPTS),
	)
	return res
}

func postHTTPRequest[T any](url, auth_token string, input any) (T, error) {
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

func postHTTPRequestAndRetryOnFail[T any](url, auth_token string, input any) T {
	var result T
	var err error
	retry.Do(
		func() error {
			result, err = postHTTPRequest[T](url, auth_token, input)
			// no error
			return err
		},
		retry.Attempts(RETRY_ATTEMPTS),
		retry.Delay(SHORT_DELAY),
	)
	return result
}
