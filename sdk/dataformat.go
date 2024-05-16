package sdk

import "github.com/soumitsalman/beansack/nlp/parrotbox"

const (
	CHANNEL = "channel"
	POST    = "post"
	ARTICLE = "article"
	COMMENT = "comment"
	INVALID = "__INVALID__"
)

type Bean struct {
	Url        string      `json:"url,omitempty" bson:"url,omitempty"`         // this is unique across each item regardless of the source and will be used as ID
	Updated    int64       `json:"updated,omitempty" bson:"updated,omitempty"` // date of update of the post or comment. Empty for subreddits
	Source     string      `json:"source,omitempty" bson:"source,omitempty"`   // which social media source is this coming from
	Title      string      `json:"title,omitempty" bson:"title,omitempty"`     // represents text title of the item. Applies to subreddits and posts but not comments
	Kind       string      `json:"kind,omitempty" bson:"kind,omitempty"`
	Text       string      `json:"text,omitempty" bson:"text,omitempty"`
	Author     string      `json:"author,omitempty" bson:"author,omitempty"`   // author of posts or comments. Empty for subreddits
	Created    int64       `json:"created,omitempty" bson:"created,omitempty"` // date of creation of the post or comment. Empty for subreddits
	MediaNoise *MediaNoise `json:"noise,omitempty" bson:"-,omitempty"`         // don't serialize this for BSON

	Keywords           []string  `json:"keywords,omitempty" bson:"keywords,omitempty"`                       // This can come from input and/or computed from a small language model
	Summary            string    `json:"summary,omitempty" bson:"summary,omitempty"`                         // generated from a large language model
	Topic              string    `json:"topic,omitempty" bson:"topic,omitempty"`                             // generated from a large language model
	SearchEmbeddings   []float32 `json:"search_embeddings,omitempty" bson:"search_embeddings,omitempty"`     // generated from a large language model
	CategoryEmbeddings []float32 `json:"category_embeddings,omitempty" bson:"category_embeddings,omitempty"` // generated from a large language model
	SearchScore        float64   `json:"search_score,omitempty" bson:"search_score,omitempty"`               // generated from DB search algorithm
}

type MediaNoise struct {
	BeanUrl       string  `json:"mapped_url,omitempty" bson:"mapped_url,omitempty"` // the id is 1:1 mapping with Bean.Id
	Updated       int64   `json:"updated,omitempty" bson:"updated,omitempty"`
	Source        string  `json:"source,omitempty" bson:"source,omitempty"` // which social media source is this coming from
	ContentId     string  `json:"cid,omitempty" bson:"cid,omitempty"`       // unique id across Source
	Name          string  `json:"name,omitempty" bson:"name,omitempty"`
	Channel       string  `json:"channel,omitempty" bson:"channel,omitempty"` // fancy name of the channel represented by the channel itself or the channel where the post/comment is
	ContainerUrl  string  `json:"container_url,omitempty" bson:"container_url,omitempty"`
	Comments      int     `json:"comments,omitempty" bson:"comments,omitempty"`       // Number of comments to a post or a comment. Doesn't apply to subreddit
	Subscribers   int     `json:"subscribers,omitempty" bson:"subscribers,omitempty"` // Number of subscribers to a channel (subreddit). Doesn't apply to posts or comments
	ThumbsupCount int     `json:"likes,omitempty" bson:"likes,omitempty"`             // number of likes, claps, thumbs-up
	ThumbsupRatio float64 `json:"likes_ratio,omitempty" bson:"likes_ratio,omitempty"` // Applies to subreddit posts and comments. Doesn't apply to subreddits
	Score         int     `json:"score,omitempty" bson:"score,omitempty"`
	Digest        string  `json:"digest,omitempty" bson:"digest,omitempty"`
}

type KeywordMap struct {
	Updated int64  `json:"updated,omitempty" bson:"updated,omitempty"`
	BeanUrl string `json:"url,omitempty" bson:"url,omitempty"`         // the id is 1:1 mapping with Bean.Id
	Keyword string `json:"keyword,omitempty" bson:"keyword,omitempty"` // extracted from a small language model
	Count   int    `bson:"count,omitempty"`
}

type NewsNugget struct {
	ID          any       `json:"_id,omitempty" bson:"_id,omitempty"`
	KeyPhrase   string    `json:"keyphrase" bson:"keyphrase,omitempty" jsonschema_description:"'keyphrase' can be the name of a company, product, person, place, security vulnerability, entity, location, organization, object, condition, acronym, documents, service, disease, medical condition, vehicle, polical group etc."`
	Event       string    `json:"event" bson:"event,omitempty" jsonschema_description:"'event' can be action, state or condition associated to the 'keyphrase' such as: what is the 'keyphrase' doing OR what is happening to the 'keyphrase' OR how is 'keyphrase' being impacted."`
	Description string    `json:"description" bson:"description,omitempty" jsonschema_description:"A concise summary of the 'event' associated to the 'keyphrase'"`
	Embeddings  []float32 `json:"-,omitempty" bson:"embeddings,omitempty"`
	Updated     int64     `json:"updated,omitempty" bson:"updated,omitempty"`
	TrendScore  int       `json:"match_count,omitempty" bson:"match_count,omitempty"`
	BeanUrls    []string  `json:"mapped_urls,omitempty" bson:"mapped_urls,omitempty"`
}

func NewKeyConcept(concept *parrotbox.KeyConcept) NewsNugget {
	return NewsNugget{
		KeyPhrase:   concept.KeyPhrase,
		Event:       concept.Event,
		Description: concept.Description,
	}
}

func Equals(a, b *Bean) bool {
	return (a.Url == b.Url)
}

// func (c *BeanChunk) PointsTo(a *Bean) bool {
// 	return c.BeanUrl == a.Url
// }

func (n *MediaNoise) PointsTo(a *Bean) bool {
	return n.BeanUrl == a.Url
}

func (k *KeywordMap) PointsTo(a *Bean) bool {
	return k.BeanUrl == a.Url
}

// func normalizeUrl(url string) string {
// 	return regexp.MustCompile("[^a-zA-Z0-9]+").ReplaceAllString(url, "-")
// }
