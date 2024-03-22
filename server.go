package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/soumitsalman/beansack/sdk"
	"golang.org/x/time/rate"
)

// PUT /beans
// GET /trending/beans?topic=keyword&window=1
// GET /trending/topics?window=1

const (
	_ERROR_MESSAGE   = "YO! do you even code?! Read this: https://github.com/soumitsalman/beansack"
	_SUCCESS_MESSAGE = "I gotchu"
)

const (
	_RATE_LIMIT = 100
	_RATE_TPS   = 2000
)

type getQueryParams struct {
	Window int      `form:"window"`
	Topics []string `form:"topic"`
}

type getBodyParams struct {
	QueryTexts      []string    `json:"query_texts"`
	QueryEmbeddings [][]float32 `json:"query_embeddings"`
	QueryKeywords   []string    `json:"query_keywords"`
}

func newBeansHandler(ctx *gin.Context) {
	var beans []sdk.Bean
	if ctx.BindJSON(&beans) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
	} else {
		go sdk.AddBeans(beans)
		ctx.String(http.StatusOK, _SUCCESS_MESSAGE)
	}
}

func getTrendingBeansHandler(ctx *gin.Context) {
	var query_params getQueryParams
	if ctx.BindQuery(&query_params) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
	} else {
		// topics := strings.Split(query_params.Topics, ",")
		var beans []sdk.Bean
		if len(query_params.Topics) > 0 {
			beans = sdk.GetBeans(query_params.Topics, query_params.Window)
		} else {
			beans = sdk.GetTrendingBeans(query_params.Window)
		}
		ctx.JSON(http.StatusOK, beans)
	}
}

func getTrendingTopicsHandler(ctx *gin.Context) {
	var query_params getQueryParams
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
	// GET /trending/beans?topic=keyword&window=1
	group.GET("/trending/beans", getTrendingBeansHandler)
	// GET /trending/topics?window=1
	group.GET("/trending/topics", getTrendingTopicsHandler)
	return router
}

func main() {
	newServer().Run()
}
