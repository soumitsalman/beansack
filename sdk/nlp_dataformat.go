package sdk

type TextEmbeddings struct {
	Text       string    `json:"text,omitempty" bson:"text,omitempty"`
	Embeddings []float32 `json:"embeddings,omitempty" bson:"embeddings,omitempty"`
}

type TextAttributes struct {
	Keywords  []string `json:"keywords,omitempty" bson:"keywords,omitempty"`   // computed from a small language model
	Summary   string   `json:"summary,omitempty" bson:"summary,omitempty"`     // computed from a small language model
	Sentiment string   `json:"sentiment,omitempty" bson:"sentiment,omitempty"` // computed from a small language model
}
