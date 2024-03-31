package main

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/soumitsalman/beansack/sdk"
	"golang.org/x/time/rate"
)

// PUT /beans
// GET /trending/beans?topic=keyword&window=1
// GET /trending/topics?window=1

const (
	_ERROR_MESSAGE   = "YO! do you even code?! Read this: https://github.com/soumitsalman/beansack."
	_SUCCESS_MESSAGE = "I gotchu."
)

const (
	_RATE_LIMIT = 100
	_RATE_TPS   = 2000
)

type queryParams struct {
	Window   int      `form:"window"`
	Topics   []string `form:"topic"`
	Trending bool     `form:"trending"`
}

type bodyParams struct {
	QueryTexts      []string    `json:"query_texts"`
	QueryEmbeddings [][]float32 `json:"query_embeddings"`
}

func newBeansHandler(ctx *gin.Context) {
	var beans []sdk.Bean
	if ctx.BindJSON(&beans) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
	} else {
		sdk.AddBeans(beans)
		ctx.String(http.StatusOK, _SUCCESS_MESSAGE)
	}
}

func getBeansHandler(ctx *gin.Context) {
	queryBeansHandler(ctx, func(filters []sdk.Option) []sdk.Bean {
		return sdk.GetBeans(filters...)
	})
}

func searchBeansHandler(ctx *gin.Context) {
	queryBeansHandler(ctx, func(filters []sdk.Option) []sdk.Bean {
		var body_params bodyParams
		if ctx.BindJSON(&body_params) != nil {
			return nil
		}
		return sdk.SearchBeans(body_params.QueryTexts, body_params.QueryEmbeddings, filters...)
	})
}

func queryBeansHandler(ctx *gin.Context, queryBeans func(filters []sdk.Option) []sdk.Bean) {
	var query_params queryParams
	if ctx.BindQuery(&query_params) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE+" query params are fucked up")
		return
	}
	// create filters
	filters := make([]sdk.Option, 0, 3)
	// Assign a time window whether it is specified or not. If it is not specified it will become 1 day
	filters = append(filters, sdk.WithTimeWindowFilter(query_params.Window))
	if query_params.Trending {
		filters = append(filters, sdk.WithTrendingFilter(query_params.Window))
	}
	if len(query_params.Topics) > 0 {
		filters = append(filters, sdk.WithKeywordsFilter(query_params.Topics))
	}

	if res := queryBeans(filters); res != nil {
		ctx.JSON(http.StatusOK, res)
	} else {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE+" The body may be fucked up")
	}
}

func getTrendingTopicsHandler(ctx *gin.Context) {
	var query_params queryParams
	if ctx.BindQuery(&query_params) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
	} else {
		ctx.JSON(http.StatusOK, sdk.GetTrendingKeywords(query_params.Window))
	}
}

func initializeRateLimiter() gin.HandlerFunc {
	limiter := rate.NewLimiter(_RATE_LIMIT, _RATE_TPS)
	return func(ctx *gin.Context) {
		if limiter.Allow() {
			ctx.Next()
		} else {
			ctx.AbortWithStatus(http.StatusTooManyRequests)
		}
	}
}

func newServer() *gin.Engine {
	router := gin.Default()
	group := router.Group("/")
	group.Use(initializeRateLimiter())
	// PUT /beans
	// TODO: put this under auth
	group.PUT("/beans", newBeansHandler)
	// GET /beans/trending?topic=keyword&window=1
	group.GET("/beans/trending", getBeansHandler)
	// GET /beans/search?window=1
	group.GET("/beans/search", searchBeansHandler)
	// GET /topics/trending?window=1
	group.GET("/topics/trending", getTrendingTopicsHandler)
	return router
}

func main() {
	if err := sdk.InitializeBeanSack(getDBConnectionString(), getParrotBoxUrl()); err != nil {
		log.Fatalln("initialization not working", err)
	}
	newServer().Run()
	// debug_main()
}
