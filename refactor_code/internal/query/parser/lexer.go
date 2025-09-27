package parser

import (
	"strings"
	"unicode"
)

// Keywords that cannot be used as symbols
var pKeywordSet = map[string]bool{
	"select": true, "from": true, "where": true, "index": true, "by": true,
	"filter": true, "limit": true, "create": true, "table": true,
	"insert": true, "into": true, "values": true, "replace": true,
	"upsert": true, "delete": true, "update": true, "set": true,
	"and": true, "or": true, "not": true, "as": true,
}

// skipSpace advances the parser past whitespace
func skipSpace(p *Parser) {
	for p.Idx < len(p.Input) && unicode.IsSpace(rune(p.Input[p.Idx])) {
		p.Idx++
	}
}

// isSymStart checks if a byte can start a symbol
func isSymStart(b byte) bool {
	return unicode.IsLetter(rune(b)) || b == '_'
}

// isSym checks if a byte can be part of a symbol
func isSym(b byte) bool {
	return unicode.IsLetter(rune(b)) || unicode.IsDigit(rune(b)) || b == '_'
}

// match multiple keywords sequentially
func pKeyword(p *Parser, kwds ...string) bool {
	save := p.Idx
	for _, kw := range kwds {
		skipSpace(p)
		end := p.Idx + len(kw)
		if end > len(p.Input) {
			p.Idx = save
			return false
		}
		// case insensitive match
		ok := strings.EqualFold(string(p.Input[p.Idx:end]), kw)
		// token is terminated
		if ok && isSym(kw[len(kw)-1]) && end < len(p.Input) {
			ok = !isSym(p.Input[end])
		}
		if !ok {
			p.Idx = save
			return false
		}
		p.Idx += len(kw)
	}
	return true
}

// pNum parses a number literal
func pNum(p *Parser, node *QLNode) bool {
	skipSpace(p)
	start := p.Idx
	if start >= len(p.Input) || !unicode.IsDigit(rune(p.Input[start])) {
		return false
	}

	// Parse integer
	for p.Idx < len(p.Input) && unicode.IsDigit(rune(p.Input[p.Idx])) {
		p.Idx++
	}

	// Convert to int64
	numStr := string(p.Input[start:p.Idx])
	var num int64
	for _, ch := range numStr {
		num = num*10 + int64(ch-'0')
	}

	node.Value.Type = QL_I64
	node.Value.I64 = num
	return true
}

// pStr parses a string literal
func pStr(p *Parser, node *QLNode) bool {
	skipSpace(p)
	if p.Idx >= len(p.Input) || p.Input[p.Idx] != '"' {
		return false
	}

	p.Idx++ // skip opening quote
	start := p.Idx

	// Find closing quote
	for p.Idx < len(p.Input) && p.Input[p.Idx] != '"' {
		if p.Input[p.Idx] == '\\' && p.Idx+1 < len(p.Input) {
			p.Idx++ // skip escape character
		}
		p.Idx++
	}

	if p.Idx >= len(p.Input) {
		return false // unclosed string
	}

	node.Value.Type = QL_STR
	node.Value.Str = p.Input[start:p.Idx]
	p.Idx++ // skip closing quote
	return true
}

// pSym parses a symbol (identifier)
func pSym(p *Parser, node *QLNode) bool {
	skipSpace(p)
	end := p.Idx
	if !(end < len(p.Input) && isSymStart(p.Input[end])) {
		return false
	}
	end++
	for end < len(p.Input) && isSym(p.Input[end]) {
		end++
	}
	if pKeywordSet[strings.ToLower(string(p.Input[p.Idx:end]))] {
		return false // not allowed
	}
	node.Value.Type = QL_SYM
	node.Value.Str = p.Input[p.Idx:end]
	p.Idx = end
	return true
}

// pQLMustSym parses a required symbol
func pQLMustSym(p *Parser) string {
	skipSpace(p)
	end := p.Idx
	if !(end < len(p.Input) && isSymStart(p.Input[end])) {
		pErr(p, nil, "expect symbol")
		return ""
	}
	end++
	for end < len(p.Input) && isSym(p.Input[end]) {
		end++
	}
	if pKeywordSet[strings.ToLower(string(p.Input[p.Idx:end]))] {
		pErr(p, nil, "keyword not allowed as symbol")
		return ""
	}
	sym := string(p.Input[p.Idx:end])
	p.Idx = end
	return sym
}
