package utils

import (
	"github.com/pkoukk/tiktoken-go"
	datautils "github.com/soumitsalman/data-utils"
)

const _DEFAULT_TEXT_LENGTH = 2048

func TruncateTextOnTokenCount(text string) string {
	tk, _ := tiktoken.GetEncoding("cl100k_base")
	return tk.Decode(
		datautils.SafeSlice(
			tk.Encode(text, nil, nil),
			0, _DEFAULT_TEXT_LENGTH,
		),
	)
}
