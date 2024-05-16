package main

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
	"github.com/soumitsalman/beansack/sdk"
	"golang.org/x/time/rate"
)

const (
	_ERROR_MESSAGE   = "YO! do you even code?! Input format is fucked. Read this: https://github.com/soumitsalman/beansack."
	_SUCCESS_MESSAGE = "I gotchu."
)

const (
	_RATE_LIMIT = 100
	_RATE_TPS   = 2000
)

// const (
// 	_SERVER_DEFAULT_WINDOW = 1
// 	_SERVER_DEFAULT_KIND   = sdk.ARTICLE
// )

type queryParams struct {
	Window int      `form:"window"`
	TopN   int      `form:"topn"`
	Kinds  []string `form:"kind"`
	// Keywords []string `form:"keyword"`
}

type bodyParams struct {
	Nuggets    []string    `json:"nuggets,omitempty"`
	Categories []string    `json:"categories,omitempty"`
	Embeddings [][]float32 `json:"embeddings,omitempty"`
	Context    string      `json:"context,omitempty"`
}

func extractParams(ctx *gin.Context) (*sdk.SearchOptions, []string) {
	options := sdk.NewQueryOptions()

	var query_params queryParams
	// if query params are mal-formed return error
	if ctx.ShouldBindQuery(&query_params) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
		return nil, nil
	}
	if len(query_params.Kinds) > 0 {
		options.WithKind(query_params.Kinds)
	}
	if query_params.Window > 0 {
		options.WithTimeWindow(query_params.Window)
	}
	if query_params.TopN > 0 {
		options.WithTopN(query_params.TopN)
	}

	var body_params bodyParams
	// if body params are provided, assign them or else proceed without them
	if ctx.ShouldBindJSON(&body_params) == nil {

		options.CategoryTexts = body_params.Categories
		options.CategoryEmbeddings = body_params.Embeddings
		options.Context = body_params.Context
	}
	return options, body_params.Nuggets
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

func searchBeansHandler(ctx *gin.Context) {
	options, nuggets := extractParams(ctx)
	if options == nil {
		return
	}

	var res []sdk.Bean
	if len(nuggets) > 0 {
		res = sdk.NuggetSearch(nuggets, options)
	} else {
		res = sdk.FuzzySearchBeans(options)
	}
	sendBeans(res, ctx)
}

func trendingBeansHandler(ctx *gin.Context) {
	options, _ := extractParams(ctx)
	if options == nil {
		return
	}
	ctx.JSON(http.StatusOK, sdk.TrendingBeans(options))
}

func trendingNuggetsHandler(ctx *gin.Context) {
	options, _ := extractParams(ctx)
	if options == nil {
		return
	}
	ctx.JSON(http.StatusOK, sdk.TrendingNuggets(options))
}

func rectifyHandler(ctx *gin.Context) {
	go sdk.Rectify()
	ctx.String(http.StatusOK, _SUCCESS_MESSAGE)
}

func serverAuthHandler(ctx *gin.Context) {
	// log.Println(ctx.GetHeader("X-API-Key"), getInternalAuthToken())
	if ctx.GetHeader("X-API-Key") == getInternalAuthToken() {
		ctx.Next()
	} else {
		ctx.AbortWithStatus(http.StatusUnauthorized)
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

func sendBeans(res []sdk.Bean, ctx *gin.Context) {
	if len(res) > 0 {
		ctx.JSON(http.StatusOK, res)
	} else {
		ctx.Status(http.StatusNoContent)
	}
}

func newServer() *gin.Engine {
	router := gin.Default()

	// SERVICE TO SERVICE AUTH
	auth_group := router.Group("/")
	auth_group.Use(initializeRateLimiter(), serverAuthHandler)
	// PUT /beans
	auth_group.PUT("/beans", newBeansHandler)
	auth_group.POST("/rectify", rectifyHandler)

	// NO NEED FOR AUTH: this is open to public
	open_group := router.Group("/")
	open_group.Use(initializeRateLimiter())
	// GET /beans/trending?window=1&keyword=amazon&keyword=apple
	open_group.GET("/beans/trending", trendingBeansHandler)
	// GET /beans/search?window=1
	open_group.GET("/beans/search", searchBeansHandler)
	// GET /nuggets/trending?window=1
	open_group.GET("/nuggets/trending", trendingNuggetsHandler)

	return router
}

func main() {
	// TODO: remove later
	godotenv.Load()

	if err := sdk.InitializeBeanSack(getDBConnectionString(), getLLMServiceAPIKey()); err != nil {
		log.Fatalln("Initialization not working", err)
	}
	// introduce an ENV VAR so that it can run as either CDN or INDEXER

	newServer().Run()
	// debug line
	// debug_main()
}
