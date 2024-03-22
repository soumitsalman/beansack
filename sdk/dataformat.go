package sdk

// import "regexp"

const (
	CHANNEL = "channel"
	POST    = "post"
	ARTICLE = "article"
	COMMENT = "comment"
)

type Bean struct {
	Url       string `json:"url,omitempty" bson:"url,omitempty"`         // this is unique across each item regardless of the source and will be used as ID
	Updated   int64  `json:"updated,omitempty" bson:"updated,omitempty"` // date of update of the post or comment. Empty for subreddits
	Source    string `json:"source,omitempty" bson:"source,omitempty"`   // which social media source is this coming from
	Title     string `json:"title,omitempty" bson:"title,omitempty"`     // represents text title of the item. Applies to subreddits and posts but not comments
	Kind      string `json:"kind,omitempty" bson:"kind,omitempty"`
	Text      string `json:"text,omitempty" bson:"text,omitempty"`
	Author    string `json:"author,omitempty" bson:"author,omitempty"`       // author of posts or comments. Empty for subreddits
	Published int64  `json:"published,omitempty" bson:"published,omitempty"` // date of creation of the post or comment. Empty for subreddits

	Keywords   []string  `json:"keywords,omitempty"`                               // DO NOT SERIALIZE IT TO MONGO. This can come from input and/or computed from a small language model
	Summary    string    `json:"summary,omitempty" bson:"summary,omitempty"`       // computed from a small language model
	Sentiment  string    `json:"sentiment,omitempty" bson:"sentiment,omitempty"`   // computed from a small language model
	Embeddings []float32 `json:"embeddings,omitempty" bson:"embeddings,omitempty"` // computed from a small language model
}

type BeanMediaNoise struct {
	BeanUrl       string  `json:"url,omitempty" bson:"url,omitempty"` // the id is 1:1 mapping with Bean.Id
	Updated       int64   `json:"updated,omitempty" bson:"updated,omitempty"`
	Media         string  `json:"media,omitempty" bson:"media,omitempty"` // which social media source is this coming from
	ContentId     string  `json:"cid,omitempty" bson:"cid,omitempty"`     // unique id across Source
	Name          string  `json:"name,omitempty" bson:"name,omitempty"`
	MediaChannel  string  `json:"channel,omitempty" bson:"channel,omitempty"` // fancy name of the channel represented by the channel itself or the channel where the post/comment is
	MediaUrl      string  `json:"mediaurl,omitempty" bson:"mediaurl,omitempty"`
	Author        string  `json:"author,omitempty" bson:"author,omitempty"`
	Comments      int     `json:"comments,omitempty" bson:"comments,omitempty"`       // Number of comments to a post or a comment. Doesn't apply to subreddit
	Subscribers   int     `json:"subscribers,omitempty" bson:"subscribers,omitempty"` // Number of subscribers to a channel (subreddit). Doesn't apply to posts or comments
	ThumbsupCount int     `json:"likes,omitempty" bson:"likes,omitempty"`             // number of likes, claps, thumbs-up
	ThumbsupRatio float64 `json:"likes_ratio,omitempty" bson:"likes_ratio,omitempty"` // Applies to subreddit posts and comments. Doesn't apply to subreddits
	Score         int     `json:"score,omitempty" bson:"score,omitempty"`
}

type KeywordMap struct {
	Updated int64  `json:"updated,omitempty" bson:"updated,omitempty"`
	BeanUrl string `json:"url,omitempty" bson:"url,omitempty"`         // the id is 1:1 mapping with Bean.Id
	Keyword string `json:"keyword,omitempty" bson:"keyword,omitempty"` // extracted from a small language model
	Count   int    `bson:"count,omitempty"`
}

func (a *Bean) Equals(b *Bean) bool {
	return (a.Url == b.Url)
}

// func (c *BeanChunk) PointsTo(a *Bean) bool {
// 	return c.BeanUrl == a.Url
// }

func (n *BeanMediaNoise) PointsTo(a *Bean) bool {
	return n.BeanUrl == a.Url
}

func (k *KeywordMap) PointsTo(a *Bean) bool {
	return k.BeanUrl == a.Url
}

// func normalizeUrl(url string) string {
// 	return regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(url, "-")
// }

type TextEmbeddings struct {
	Text       string    `json:"text,omitempty" bson:"text,omitempty"`
	Embeddings []float32 `json:"embeddings,omitempty" bson:"embeddings,omitempty"`
}

type TextAttributes struct {
	Keywords  []string `json:"keywords,omitempty" bson:"keywords,omitempty"`   // computed from a small language model
	Summary   string   `json:"summary,omitempty" bson:"summary,omitempty"`     // computed from a small language model
	Sentiment string   `json:"sentiment,omitempty" bson:"sentiment,omitempty"` // computed from a small language model
}
