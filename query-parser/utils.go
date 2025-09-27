package queryparser

import "unicode"

var pKeywordSet = map[string]bool{
	"from":   true,
	"index":  true,
	"filter": true,
	"limit":  true,
}

func isSym(ch byte) bool {
	r := rune(ch)
	return unicode.IsLetter(r) || unicode.IsNumber(r) || r == '_'
}
func isSymStart(ch byte) bool {
	return unicode.IsLetter(rune(ch)) || ch == '_' || ch == '@'
}

func (p *Parser) skipSpace() {
	for p.idx < len(p.input) && unicode.IsSpace(rune(p.input[p.idx])) {
		p.idx++
	}
}