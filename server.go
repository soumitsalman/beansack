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
	_ERROR_MESSAGE   = "YO! do you even code?! Input format is fucked. Read this: https://github.com/soumitsalman/beansack."
	_SUCCESS_MESSAGE = "I gotchu."
)

const (
	_RATE_LIMIT = 100
	_RATE_TPS   = 2000
)

type queryParams struct {
	Window   int      `form:"window"`
	Keywords []string `form:"keyword"`
}

type bodyParams struct {
	QueryTexts    []string `json:"query_texts,omitempty"`
	SearchContext string   `json:"search_context,omitempty"`
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
	var query_params queryParams
	if ctx.BindQuery(&query_params) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
		return
	}

	var res []sdk.Bean
	if len(query_params.Keywords) > 0 {
		res = sdk.GetBeans(sdk.WithTimeWindowFilter(query_params.Window), sdk.WithKeywordsFilter(query_params.Keywords))
	} else {
		res = sdk.GetBeans(sdk.WithTrendingFilter(query_params.Window))
	}
	ctx.JSON(http.StatusOK, res)
}

func searchBeansHandler(ctx *gin.Context) {
	var query_params queryParams
	var body_params bodyParams
	if ctx.BindQuery(&query_params) != nil || ctx.BindJSON(&body_params) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
		return
	}

	var res []sdk.Bean
	if len(body_params.QueryTexts) > 0 {
		res = sdk.TextSearchBeans(body_params.QueryTexts, sdk.WithTimeWindowFilter(query_params.Window))
	} else if len(body_params.SearchContext) > 0 {
		res = sdk.SimilaritySearchBeans(body_params.SearchContext, sdk.WithTimeWindowFilter(query_params.Window))
	}

	ctx.JSON(http.StatusOK, res)
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

	// GET /beans/trending?window=1&keyword=amazon&keyword=apple
	group.GET("/beans/trending", getBeansHandler)

	// GET /beans/search?window=1
	// query_texts: []string
	// similarity_text: string
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
