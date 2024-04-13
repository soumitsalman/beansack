package sdk

import (
	"github.com/soumitsalman/beansack/store"
	datautils "github.com/soumitsalman/data-utils"
)

type Option func(filter store.JSON)

func WithKeywordsFilter(keywords []string) Option {
	return func(filter store.JSON) {
		filter["keywords"] = store.JSON{"$in": keywords}
	}
}

func WithTrendingFilter(time_window int) Option {
	return func(filter store.JSON) {
		keywords := datautils.Transform(GetTrendingKeywords(time_window), func(item *KeywordMap) string { return item.Keyword })
		filter["keywords"] = store.JSON{"$in": keywords}
		filter["updated"] = store.JSON{"$gte": timeValue(checkAndFixTimeWindow(time_window))}
	}
}

func WithTimeWindowFilter(time_window int) Option {
	return func(filter store.JSON) {
		filter["updated"] = store.JSON{"$gte": timeValue(checkAndFixTimeWindow(time_window))}
	}
}

func WithKindFilter(kinds []string) Option {
	return func(filter store.JSON) {
		filter["kind"] = store.JSON{"$in": kinds}
	}
}
