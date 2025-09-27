package queryparser

import (
	"fmt"
	"govetachun/go-mini-db/utils"
	"strings"
	"unicode"
)

func pExprTuple(p *Parser, node *QLNode) {
	kids := []QLNode{{}}
	pExprOr(p, &kids[len(kids)-1])
	for pKeyword(p, ",") {
		kids = append(kids, QLNode{})
		pExprOr(p, &kids[len(kids)-1])
	}
	if len(kids) > 1 {
		node.Value.Type = QL_TUP
		node.Kids = kids
	} else {
		*node = kids[0] // not a tuple
	}
}

// match multiple keywords sequentially
func pKeyword(p *Parser, kwds ...string) bool {
	save := p.idx
	for _, kw := range kwds {
		p.skipSpace()
		end := p.idx + len(kw)
		if end > len(p.input) {
			p.idx = save
			return false
		}
		// case insensitive matach
		ok := strings.EqualFold(string(p.input[p.idx:end]), kw)
		// token is terminated
		if ok && isSym(kw[len(kw)-1]) && end < len(p.input) {
			ok = !isSym(p.input[end])
		}
		if !ok {
			p.idx = save
			return false
		}
		p.idx += len(kw)
	}
	return true
}

func pExprOr(p *Parser, node *QLNode) {
	pExprBinop(p, node, []string{"or"}, []uint32{QL_CMP_OR}, pExprAnd)
}
func pExprBinop(
	p *Parser, node *QLNode,
	ops []string, types []uint32, next func(*Parser, *QLNode),
) {
	utils.Assert(len(ops) == len(types), "len(ops) == len(types)")
	left := QLNode{}
	next(p, &left)
	for more := true; more; {
		more = false
		for i := range ops {
			if pKeyword(p, ops[i]) {
				new := QLNode{Value: Value{Type: types[i]}}
				new.Kids = []QLNode{left, {}}
				next(p, &new.Kids[1])
				left = new
				more = true
				break
			}
		}
	}
	*node = left
}

func pExprAnd(p *Parser, node *QLNode) {
	pExprBinop(p, node, []string{"and"}, []uint32{QL_CMP_AND}, pExprNot)
}

func pExprNot(p *Parser, node *QLNode) {
	switch {
	case pKeyword(p, "not"):
		node.Value.Type = QL_NOT
		node.Kids = []QLNode{{}}
		pExprCmp(p, &node.Kids[0])
	default:
		pExprCmp(p, node)
	}
}

func pExprCmp(p *Parser, node *QLNode) {
	pExprBinop(p, node,
		[]string{"<=", ">=", "<", ">", "=", "!="},
		[]uint32{QL_CMP_LE, QL_CMP_GE, QL_CMP_LT, QL_CMP_GT, QL_CMP_EQ, QL_CMP_NE},
		pExprAdd)
}

func pExprAdd(p *Parser, node *QLNode) {
	pExprBinop(p, node,
		[]string{"+", "-"},
		[]uint32{QL_CMP_ADD, QL_CMP_SUB},
		pExprMul)
}
func pExprMul(p *Parser, node *QLNode) {
	pExprBinop(p, node,
		[]string{"*", "/", "%"}, []uint32{QL_CMP_MUL, QL_CMP_DIV, QL_CMP_MOD}, pExprUnop)
}

func pExprUnop(p *Parser, node *QLNode) {
	switch {
	case pKeyword(p, "-"):
		node.Value.Type = QL_NEG
		node.Kids = []QLNode{{}}
		pExprAtom(p, &node.Kids[0])
	default:
		pExprAtom(p, node)
	}
}
func pExprAtom(p *Parser, node *QLNode) {
	switch {
	case pKeyword(p, "("):
		pExprTuple(p, node)
		if !pKeyword(p, ")") {
			pErr(p, node, "unclosed parenthesis")
		}
	case pSym(p, node):
	case pNum(p, node):
	case pStr(p, node):
	default:
		pErr(p, node, "expect symbol, number or string")
	}
}

func pErr(p *Parser, node *QLNode, msg string) {
	node.Value.Type = QL_ERR
	node.Value.Str = []byte(msg)
	p.err = fmt.Errorf("parse error: %s", msg)
}

func pNum(p *Parser, node *QLNode) bool {
	p.skipSpace()
	start := p.idx
	if start >= len(p.input) || !unicode.IsDigit(rune(p.input[start])) {
		return false
	}

	// Parse integer
	for p.idx < len(p.input) && unicode.IsDigit(rune(p.input[p.idx])) {
		p.idx++
	}

	// Convert to int64
	numStr := string(p.input[start:p.idx])
	var num int64
	for _, ch := range numStr {
		num = num*10 + int64(ch-'0')
	}

	node.Value.Type = QL_I64
	node.Value.I64 = num
	return true
}

func pStr(p *Parser, node *QLNode) bool {
	p.skipSpace()
	if p.idx >= len(p.input) || p.input[p.idx] != '"' {
		return false
	}

	p.idx++ // skip opening quote
	start := p.idx

	// Find closing quote
	for p.idx < len(p.input) && p.input[p.idx] != '"' {
		if p.input[p.idx] == '\\' && p.idx+1 < len(p.input) {
			p.idx++ // skip escape character
		}
		p.idx++
	}

	if p.idx >= len(p.input) {
		return false // unclosed string
	}

	node.Value.Type = QL_STR
	node.Value.Str = p.input[start:p.idx]
	p.idx++ // skip closing quote
	return true
}

func pSym(p *Parser, node *QLNode) bool {
	p.skipSpace()
	end := p.idx
	if !(end < len(p.input) && isSymStart(p.input[end])) {
		return false
	}
	end++
	for end < len(p.input) && isSym(p.input[end]) {
		end++
	}
	if pKeywordSet[strings.ToLower(string(p.input[p.idx:end]))] {
		return false // not allowed
	}
	node.Value.Type = QL_SYM
	node.Value.Str = p.input[p.idx:end]
	p.idx = end
	return true
}
