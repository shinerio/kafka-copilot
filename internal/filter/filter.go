package filter

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/itchyny/gojq"
)

// Message represents a Kafka message for filtering.
type Message struct {
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Timestamp time.Time         `json:"timestamp"`
	Key       []byte            `json:"-"`
	Value     []byte            `json:"-"`
	Headers   map[string][]byte `json:"-"`
	Size      int               `json:"size"`
}

// FilterConfig holds filter parameters.
type FilterConfig struct {
	Key         string // exact key match
	KeyRegex    string // regex key match
	Keyword     string // value keyword (substring)
	IgnoreCase  bool   // case-insensitive keyword (default true)
	Expr        string // jq expression

	MaxValueDisplay int
}

// Decoder is the plugin interface for value decoding (D16).
type Decoder interface {
	Decode(data []byte) (any, error)
	Name() string
}

// JSONDecoder decodes JSON values.
type JSONDecoder struct{}

func (JSONDecoder) Name() string { return "json" }
func (JSONDecoder) Decode(data []byte) (any, error) {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}
	return v, nil
}

// TextDecoder decodes as plain text.
type TextDecoder struct{}

func (TextDecoder) Name() string { return "text" }
func (TextDecoder) Decode(data []byte) (any, error) {
	return string(data), nil
}

// Filter applies three-layer filtering to messages.
type Filter struct {
	config    FilterConfig
	decoder   Decoder
	keyRegex  *regexp.Regexp
	jqCode    *gojq.Code

	SkippedNonUTF8Key int
}

// New creates a filter from config.
func New(cfg FilterConfig) (*Filter, error) {
	f := &Filter{
		config:  cfg,
		decoder: JSONDecoder{},
	}
	if cfg.KeyRegex != "" {
		re, err := regexp.Compile(cfg.KeyRegex)
		if err != nil {
			return nil, fmt.Errorf("key-regex compile error: %w", err)
		}
		f.keyRegex = re
	}
	if cfg.Expr != "" {
		query, err := gojq.Parse(cfg.Expr)
		if err != nil {
			return nil, fmt.Errorf("jq expression parse error: %w", err)
		}
		f.jqCode, err = gojq.Compile(query)
		if err != nil {
			return nil, fmt.Errorf("jq expression compile error: %w", err)
		}
	}
	return f, nil
}

// Result holds filter result for a single message.
type Result struct {
	Matched   bool              `json:"-"`
	Key       string            `json:"key"`
	Value     any               `json:"value"`
	RawValue  []byte            `json:"-"`
	Truncated bool              `json:"truncated,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	Partition int32             `json:"partition"`
	Offset    int64             `json:"offset"`
	Timestamp time.Time         `json:"timestamp"`
	Size      int               `json:"size"`
}

// Apply applies all three filter layers to a message.
func (f *Filter) Apply(msg *Message) (*Result, bool) {
	r := &Result{
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
		RawValue:  msg.Value,
		Size:      msg.Size,
	}

	// Decode key
	keyStr, keyValid := tryUTF8(msg.Key)
	if !keyValid && len(msg.Key) > 0 {
		r.Key = "hex:" + hex.EncodeToString(msg.Key)
		f.SkippedNonUTF8Key++
		if f.config.Key != "" || f.config.KeyRegex != "" {
			return r, false
		}
	} else {
		r.Key = keyStr
	}

	// Layer 1: key filter
	if f.config.Key != "" {
		if keyStr != f.config.Key {
			return r, false
		}
	}
	if f.keyRegex != nil {
		if !f.keyRegex.MatchString(keyStr) {
			return r, false
		}
	}

	// Decode value
	var decoded any
	if len(msg.Value) > 0 {
		v, err := f.decoder.Decode(msg.Value)
		if err != nil {
			decoded = string(msg.Value)
		} else {
			decoded = v
		}
	}
	r.Value = decoded

	// Truncate large values
	maxDisplay := f.config.MaxValueDisplay
	if maxDisplay <= 0 {
		maxDisplay = 64 * 1024
	}
	if len(msg.Value) > 1024*1024 {
		r.Truncated = true
		r.RawValue = msg.Value[:maxDisplay]
		if v, err := f.decoder.Decode(r.RawValue); err == nil {
			r.Value = v
		} else {
			r.Value = string(r.RawValue)
		}
	}

	// Decode headers
	r.Headers = make(map[string]string)
	for k, v := range msg.Headers {
		if s, ok := tryUTF8Str(v); ok {
			r.Headers[k] = s
		} else {
			r.Headers[k] = "hex:" + hex.EncodeToString(v)
		}
	}

	// Layer 2: keyword filter
	if f.config.Keyword != "" {
		valueStr := toString(decoded)
		if f.config.IgnoreCase {
			if !strings.Contains(strings.ToLower(valueStr), strings.ToLower(f.config.Keyword)) {
				return r, false
			}
		} else {
			if !strings.Contains(valueStr, f.config.Keyword) {
				return r, false
			}
		}
	}

	// Layer 3: jq expression filter
	if f.jqCode != nil {
		ctx := map[string]any{
			"key":       keyStr,
			"value":     decoded,
			"headers":   r.Headers,
			"partition": msg.Partition,
			"offset":    msg.Offset,
			"timestamp": msg.Timestamp.Format(time.RFC3339Nano),
			"size":      msg.Size,
		}
		matched := false
		iter := f.jqCode.Run(ctx)
		for {
			v, ok := iter.Next()
			if !ok {
				break
			}
			if _, isErr := v.(error); isErr {
				break
			}
			if isTruthy(v) {
				matched = true
				break
			}
		}
		if !matched {
			return r, false
		}
	}

	r.Matched = true
	return r, true
}

func isTruthy(v any) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case float64:
		return val != 0
	case int:
		return val != 0
	case string:
		return val != "" && val != "false" && val != "null"
	case []interface{}:
		return len(val) > 0
	case map[string]interface{}:
		return len(val) > 0
	default:
		return true
	}
}

func tryUTF8(b []byte) (string, bool) {
	if len(b) == 0 {
		return "", true
	}
	s := string(b)
	for i := 0; i < len(b); {
		r, size := decodeRune(b[i:])
		if r == 0xFFFD && size == 1 {
			return "", false
		}
		i += size
	}
	return s, true
}

func tryUTF8Str(b []byte) (string, bool) {
	return tryUTF8(b)
}

func decodeRune(b []byte) (rune, int) {
	if len(b) == 0 {
		return 0, 0
	}
	if b[0] < 0x80 {
		return rune(b[0]), 1
	}
	if b[0]&0xE0 == 0xC0 {
		if len(b) < 2 {
			return 0xFFFD, 1
		}
		return rune(b[0]&0x1F)<<6 | rune(b[1]&0x3F), 2
	}
	if b[0]&0xF0 == 0xE0 {
		if len(b) < 3 {
			return 0xFFFD, 1
		}
		return rune(b[0]&0x0F)<<12 | rune(b[1]&0x3F)<<6 | rune(b[2]&0x3F), 3
	}
	if b[0]&0xF8 == 0xF0 {
		if len(b) < 4 {
			return 0xFFFD, 1
		}
		return rune(b[0]&0x07)<<18 | rune(b[1]&0x3F)<<12 | rune(b[2]&0x3F)<<6 | rune(b[3]&0x3F), 4
	}
	return 0xFFFD, 1
}

func toString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case nil:
		return ""
	default:
		b, _ := json.Marshal(val)
		return string(b)
	}
}

const DefaultMaxValueDisplay = 64 * 1024
