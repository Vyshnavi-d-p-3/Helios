package promql

import (
	"fmt"
	"unicode"
)

// tokenKind enumerates lexical categories.
type tokenKind int

const (
	tkEOF tokenKind = iota
	tkIdent
	tkNumber
	tkString
	tkLParen
	tkRParen
	tkLBrace
	tkRBrace
	tkLBracket
	tkRBracket
	tkComma
	tkEqual
	tkNotEqual
	tkRegex
	tkNotRegex
	tkBy
	tkWithout // accepted but not implemented; produces clearer errors than "unexpected token"
	tkPlus
	tkMinus
)

type token struct {
	kind tokenKind
	val  string
	pos  int
}

func (t token) String() string {
	if t.val != "" {
		return fmt.Sprintf("%s(%q)", kindName(t.kind), t.val)
	}
	return kindName(t.kind)
}

func kindName(k tokenKind) string {
	switch k {
	case tkEOF:
		return "EOF"
	case tkIdent:
		return "IDENT"
	case tkNumber:
		return "NUMBER"
	case tkString:
		return "STRING"
	case tkLParen:
		return "("
	case tkRParen:
		return ")"
	case tkLBrace:
		return "{"
	case tkRBrace:
		return "}"
	case tkLBracket:
		return "["
	case tkRBracket:
		return "]"
	case tkComma:
		return ","
	case tkEqual:
		return "="
	case tkNotEqual:
		return "!="
	case tkRegex:
		return "=~"
	case tkNotRegex:
		return "!~"
	case tkBy:
		return "BY"
	case tkWithout:
		return "WITHOUT"
	case tkPlus:
		return "+"
	case tkMinus:
		return "-"
	}
	return "?"
}

// lexer is a forward-only tokenizer. It does not look ahead more than one byte.
type lexer struct {
	src  string
	pos  int
	peek *token // single-token putback
}

func newLexer(src string) *lexer {
	return &lexer{src: src}
}

func (l *lexer) peekTok() token {
	if l.peek != nil {
		return *l.peek
	}
	t := l.next()
	l.peek = &t
	return t
}

func (l *lexer) next() token {
	if l.peek != nil {
		t := *l.peek
		l.peek = nil
		return t
	}
	l.skipWhitespace()
	if l.pos >= len(l.src) {
		return token{kind: tkEOF, pos: l.pos}
	}
	start := l.pos
	c := l.src[l.pos]

	// Single-char tokens
	switch c {
	case '(':
		l.pos++
		return token{kind: tkLParen, pos: start}
	case ')':
		l.pos++
		return token{kind: tkRParen, pos: start}
	case '{':
		l.pos++
		return token{kind: tkLBrace, pos: start}
	case '}':
		l.pos++
		return token{kind: tkRBrace, pos: start}
	case '[':
		l.pos++
		return token{kind: tkLBracket, pos: start}
	case ']':
		l.pos++
		return token{kind: tkRBracket, pos: start}
	case ',':
		l.pos++
		return token{kind: tkComma, pos: start}
	case '+':
		l.pos++
		return token{kind: tkPlus, pos: start}
	case '-':
		l.pos++
		return token{kind: tkMinus, pos: start}
	}

	// Two-char operators
	if c == '=' {
		if l.pos+1 < len(l.src) && l.src[l.pos+1] == '~' {
			l.pos += 2
			return token{kind: tkRegex, pos: start}
		}
		l.pos++
		return token{kind: tkEqual, pos: start}
	}
	if c == '!' {
		if l.pos+1 < len(l.src) {
			next := l.src[l.pos+1]
			if next == '=' {
				l.pos += 2
				return token{kind: tkNotEqual, pos: start}
			}
			if next == '~' {
				l.pos += 2
				return token{kind: tkNotRegex, pos: start}
			}
		}
	}

	// String literal
	if c == '"' {
		return l.scanString(start)
	}

	// Number
	if isDigit(c) || (c == '.' && l.pos+1 < len(l.src) && isDigit(l.src[l.pos+1])) {
		return l.scanNumber(start)
	}

	// Identifier or keyword
	if isIdentStart(c) {
		return l.scanIdent(start)
	}

	// Unknown — return a one-byte token tagged as ident so the parser produces
	// a proper position-aware error rather than panicking.
	l.pos++
	return token{kind: tkIdent, val: string(c), pos: start}
}

func (l *lexer) scanString(start int) token {
	l.pos++ // consume opening "
	var out []byte
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if c == '\\' && l.pos+1 < len(l.src) {
			esc := l.src[l.pos+1]
			switch esc {
			case 'n':
				out = append(out, '\n')
			case 't':
				out = append(out, '\t')
			case 'r':
				out = append(out, '\r')
			case '\\':
				out = append(out, '\\')
			case '"':
				out = append(out, '"')
			default:
				out = append(out, esc)
			}
			l.pos += 2
			continue
		}
		if c == '"' {
			l.pos++
			return token{kind: tkString, val: string(out), pos: start}
		}
		out = append(out, c)
		l.pos++
	}
	// Unterminated — return what we have; parser raises.
	return token{kind: tkString, val: string(out), pos: start}
}

func (l *lexer) scanNumber(start int) token {
	dot := false
	for l.pos < len(l.src) {
		c := l.src[l.pos]
		if isDigit(c) {
			l.pos++
			continue
		}
		if c == '.' && !dot {
			dot = true
			l.pos++
			continue
		}
		// Allow scientific notation: 1e9, 1.5E-3
		if (c == 'e' || c == 'E') && l.pos > start {
			l.pos++
			if l.pos < len(l.src) && (l.src[l.pos] == '+' || l.src[l.pos] == '-') {
				l.pos++
			}
			continue
		}
		break
	}
	// Allow trailing duration suffix to fall through as a separate token? No —
	// duration parsing happens in scanIdent context (after number) for ranges.
	return token{kind: tkNumber, val: l.src[start:l.pos], pos: start}
}

func (l *lexer) scanIdent(start int) token {
	for l.pos < len(l.src) && isIdentCont(l.src[l.pos]) {
		l.pos++
	}
	val := l.src[start:l.pos]
	// Keyword recognition
	switch val {
	case "by":
		return token{kind: tkBy, val: val, pos: start}
	case "without":
		return token{kind: tkWithout, val: val, pos: start}
	}
	return token{kind: tkIdent, val: val, pos: start}
}

// scanDuration is invoked by the parser after consuming '['. It reads digits
// and a unit suffix (s, m, h, d, w, ms) and returns the raw text. Returning
// the text rather than a parsed time.Duration keeps the lexer dumb.
func (l *lexer) scanDuration() (string, int, error) {
	l.skipWhitespace()
	start := l.pos
	// digits
	for l.pos < len(l.src) && isDigit(l.src[l.pos]) {
		l.pos++
	}
	if l.pos == start {
		return "", start, fmt.Errorf("expected duration digits")
	}
	// unit
	unitStart := l.pos
	for l.pos < len(l.src) && isIdentCont(l.src[l.pos]) {
		l.pos++
	}
	if l.pos == unitStart {
		return "", start, fmt.Errorf("expected duration unit")
	}
	return l.src[start:l.pos], start, nil
}

func (l *lexer) skipWhitespace() {
	for l.pos < len(l.src) && unicode.IsSpace(rune(l.src[l.pos])) {
		l.pos++
	}
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

func isIdentCont(c byte) bool {
	return c == '_' || c == ':' || isDigit(c) || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
