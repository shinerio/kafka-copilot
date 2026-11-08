package filter

import "strings"

// Keyword expression evaluator for the `--keyword` filter.
//
// Supports a small boolean expression language so users can combine multiple
// terms with AND / OR / NOT and group with parentheses. Precedence:
//   OR  (lowest)  <  AND  <  NOT  <  parentheses / term (highest)
//
// A bare term (quoted string or bare word) is a case-insensitive substring
// match against the message's searchable text (key + value + headers).
//
// Examples:
//   foo
//   foo AND bar
//   foo OR (bar AND NOT baz)
//   "error code" AND retry

type kwNode interface{ eval(text string) bool }

type kwTerm struct{ word string }
type kwNot struct{ child kwNode }
type kwAnd struct{ left, right kwNode }
type kwOr struct{ left, right kwNode }

func (t kwTerm) eval(text string) bool {
	return strings.Contains(strings.ToLower(text), strings.ToLower(t.word))
}
func (n kwNot) eval(text string) bool { return !n.child.eval(text) }
func (a kwAnd) eval(text string) bool { return a.left.eval(text) && a.right.eval(text) }
func (o kwOr) eval(text string) bool  { return o.left.eval(text) || o.right.eval(text) }

type kwTokKind int

const (
	kwParen kwTokKind = iota
	kwAndTok
	kwOrTok
	kwNotTok
	kwWord
)

type kwToken struct {
	kind kwTokKind
	val  string
}

func tokenizeKW(s string) []kwToken {
	var toks []kwToken
	i, n := 0, len(s)
	for i < n {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(' || c == ')':
			toks = append(toks, kwToken{kind: kwParen, val: string(c)})
			i++
		case c == '"' || c == '\'':
			quote := c
			i++
			start := i
			for i < n && s[i] != quote {
				i++
			}
			toks = append(toks, kwToken{kind: kwWord, val: s[start:i]})
			if i < n {
				i++ // skip closing quote
			}
		default:
			start := i
			for i < n && s[i] != ' ' && s[i] != '\t' && s[i] != '\n' && s[i] != '\r' &&
				s[i] != '(' && s[i] != ')' && s[i] != '"' && s[i] != '\'' {
				i++
			}
			word := s[start:i]
			switch strings.ToUpper(word) {
			case "AND":
				toks = append(toks, kwToken{kind: kwAndTok})
			case "OR":
				toks = append(toks, kwToken{kind: kwOrTok})
			case "NOT":
				toks = append(toks, kwToken{kind: kwNotTok})
			default:
				toks = append(toks, kwToken{kind: kwWord, val: word})
			}
		}
	}
	return toks
}

type kwParser struct {
	toks []kwToken
	pos  int
}

func (p *kwParser) peek() (kwToken, bool) {
	if p.pos < len(p.toks) {
		return p.toks[p.pos], true
	}
	return kwToken{}, false
}
func (p *kwParser) next() (kwToken, bool) {
	t, ok := p.peek()
	if ok {
		p.pos++
	}
	return t, ok
}

func (p *kwParser) parseOr() (kwNode, bool) {
	left, ok := p.parseAnd()
	if !ok {
		return nil, false
	}
	for {
		t, ok2 := p.peek()
		if !ok2 || t.kind != kwOrTok {
			break
		}
		p.next()
		right, ok3 := p.parseAnd()
		if !ok3 {
			return nil, false
		}
		left = kwOr{left, right}
	}
	return left, true
}

func (p *kwParser) parseAnd() (kwNode, bool) {
	left, ok := p.parseNot()
	if !ok {
		return nil, false
	}
	for {
		t, ok2 := p.peek()
		if !ok2 || t.kind != kwAndTok {
			break
		}
		p.next()
		right, ok3 := p.parseNot()
		if !ok3 {
			return nil, false
		}
		left = kwAnd{left, right}
	}
	return left, true
}

func (p *kwParser) parseNot() (kwNode, bool) {
	t, ok := p.peek()
	if !ok {
		return nil, false
	}
	if t.kind == kwNotTok {
		p.next()
		child, ok2 := p.parseNot()
		if !ok2 {
			return nil, false
		}
		return kwNot{child}, true
	}
	return p.parsePrimary()
}

func (p *kwParser) parsePrimary() (kwNode, bool) {
	t, ok := p.next()
	if !ok {
		return nil, false
	}
	if t.kind == kwParen && t.val == "(" {
		node, ok2 := p.parseOr()
		if !ok2 {
			return nil, false
		}
		closeT, ok3 := p.next()
		if !ok3 || closeT.kind != kwParen || closeT.val != ")" {
			return nil, false
		}
		return node, true
	}
	if t.kind == kwWord {
		return kwTerm{word: t.val}, true
	}
	return nil, false
}

// CompileKeyword parses a keyword expression into an evaluable node.
// ok=false indicates an invalid expression (callers should fall back to a
// plain substring match on the whole string).
func CompileKeyword(expr string) (kwNode, bool) {
	if strings.TrimSpace(expr) == "" {
		return nil, false
	}
	toks := tokenizeKW(expr)
	if len(toks) == 0 {
		return nil, false
	}
	p := &kwParser{toks: toks}
	node, ok := p.parseOr()
	if !ok || p.pos != len(toks) {
		return nil, false
	}
	return node, true
}

// buildSearchText assembles the text a keyword term is matched against:
// key + value (stringified) + headers. Case-insensitive substring search.
func buildSearchText(keyStr string, decoded any, headers map[string]string) string {
	var sb strings.Builder
	sb.WriteString(keyStr)
	sb.WriteByte(' ')
	sb.WriteString(toString(decoded))
	for k, v := range headers {
		sb.WriteByte(' ')
		sb.WriteString(k)
		sb.WriteByte(':')
		sb.WriteString(v)
	}
	return sb.String()
}
