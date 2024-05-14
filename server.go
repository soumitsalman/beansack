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

const (
	_SERVER_DEFAULT_WINDOW = 1
	_SERVER_DEFAULT_KIND   = sdk.ARTICLE
)

type queryParams struct {
	Window   int      `form:"window"`
	Keywords []string `form:"keyword"`
	Kinds    []string `form:"kind"`
}

type bodyParams struct {
	NewsNuggets         []string `json:"newsnuggets,omitempty"`
	Categories          []string `json:"categories,omitempty"`
	ConversationContext string   `json:"search_text,omitempty"`
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
	filters := extractFilters(ctx)
	if filters == nil {
		return
	}

	res := sdk.GetBeans(filters...)
	sendBeans(res, ctx)
}

func searchBeansHandler(ctx *gin.Context) {
	filters := extractFilters(ctx)
	if filters == nil {
		return
	}

	var body_params bodyParams
	if ctx.BindJSON(&body_params) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
		return
	}

	var res []sdk.Bean
	if len(body_params.Categories) > 0 {
		res = sdk.CategorySearch(body_params.Categories, filters...)
	} else if len(body_params.ConversationContext) > 0 {
		res = sdk.ConversationContextSearch(body_params.ConversationContext, filters...)
	} else if len(body_params.NewsNuggets) > 0 {
		res = sdk.NuggetSearch(body_params.NewsNuggets, filters...)
	}

	sendBeans(res, ctx)
}

func sendBeans(res []sdk.Bean, ctx *gin.Context) {
	if len(res) > 0 {
		ctx.JSON(http.StatusOK, res)
	} else {
		ctx.Status(http.StatusNoContent)
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

func getTrendingNuggetsHandler(ctx *gin.Context) {
	var query_params queryParams
	if ctx.BindQuery(&query_params) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
	} else {
		ctx.JSON(http.StatusOK, sdk.GetTrendingNewsNuggets(query_params.Window))
	}
}

func rectifyHandler(ctx *gin.Context) {
	go sdk.RectifyBeans()
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

func extractFilters(ctx *gin.Context) []sdk.Option {
	var query_params queryParams
	if ctx.BindQuery(&query_params) != nil {
		ctx.String(http.StatusBadRequest, _ERROR_MESSAGE)
		return nil
	}

	filters := make([]sdk.Option, 0, 3)
	if len(query_params.Kinds) > 0 {
		filters = append(filters, sdk.WithKindFilter(query_params.Kinds))
	}
	if query_params.Window > 0 {
		filters = append(filters, sdk.WithTimeWindowFilter(query_params.Window))
	}
	if len(query_params.Keywords) > 0 {
		filters = append(filters, sdk.WithKeywordsFilter(query_params.Keywords))
	}
	return filters
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
	open_group.GET("/beans/trending", getBeansHandler)
	// GET /beans/search?window=1
	// query_texts: []string
	// similarity_text: string
	open_group.GET("/beans/search", searchBeansHandler)
	// GET /topics/trending?window=1
	open_group.GET("/topics/trending", getTrendingTopicsHandler)
	// GET /nuggets/trending?window=1
	open_group.GET("/nuggets/trending", getTrendingTopicsHandler)

	return router
}

func main() {
	// TODO: remove later
	godotenv.Load()

	if err := sdk.InitializeBeanSack(getDBConnectionString(), getLLMServiceAPIKey()); err != nil {
		log.Fatalln("Initialization not working", err)
	}
	newServer().Run()
	// debug line
	// debug_main()
}
