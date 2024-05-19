package sdk

import (
	"time"

	"github.com/soumitsalman/beansack/store"
)

const (
	_DEFAULT_TOPN = 10
	_MAX_TOPN     = 100
)

type SearchOptions struct {
	Filter             store.JSON
	TopN               int
	CategoryTexts      []string
	CategoryEmbeddings [][]float32
	Context            string
}

func NewSearchOptions() *SearchOptions {
	return &SearchOptions{
		Filter: make(store.JSON),
		TopN:   _DEFAULT_TOPN,
	}
}

func (settings *SearchOptions) WithTimeWindow(time_window int) *SearchOptions {
	settings.Filter["updated"] = store.JSON{"$gte": timeValue(time_window)}
	return settings
}

func (settings *SearchOptions) WithKind(kinds []string) *SearchOptions {
	settings.Filter["kind"] = store.JSON{"$in": kinds}
	return settings
}

func (settings *SearchOptions) WithTopN(topn int) *SearchOptions {
	if topn <= 0 {
		settings.TopN = _DEFAULT_TOPN
	} else if topn > _MAX_TOPN {
		settings.TopN = _MAX_TOPN
	} else {
		settings.TopN = topn
	}
	return settings
}

func timeValue(time_window int) int64 {
	return time.Now().AddDate(0, 0, -checkAndFixTimeWindow(time_window)).Unix()
}

func checkAndFixTimeWindow(time_window int) int {
	switch {
	case time_window > _FOUR_WEEKS:
		return _FOUR_WEEKS
	case time_window < _ONE_DAY:
		return _ONE_DAY
	default:
		return time_window
	}
}
