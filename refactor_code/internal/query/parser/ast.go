package parser

import "govetachun/go-mini-db/refactor_code/pkg/utils"

// pExprTuple parses tuple expressions or single expressions
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

// pExprOr parses OR expressions
func pExprOr(p *Parser, node *QLNode) {
	pExprBinop(p, node, []string{"or"}, []uint32{QL_CMP_OR}, pExprAnd)
}

// pExprBinop is a generic binary operator parser
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

// pExprAnd parses AND expressions
func pExprAnd(p *Parser, node *QLNode) {
	pExprBinop(p, node, []string{"and"}, []uint32{QL_CMP_AND}, pExprNot)
}

// pExprNot parses NOT expressions
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

// pExprCmp parses comparison expressions
func pExprCmp(p *Parser, node *QLNode) {
	pExprBinop(p, node,
		[]string{"<=", ">=", "<", ">", "=", "!="},
		[]uint32{QL_CMP_LE, QL_CMP_GE, QL_CMP_LT, QL_CMP_GT, QL_CMP_EQ, QL_CMP_NE},
		pExprAdd)
}

// pExprAdd parses addition and subtraction expressions
func pExprAdd(p *Parser, node *QLNode) {
	pExprBinop(p, node,
		[]string{"+", "-"},
		[]uint32{QL_CMP_ADD, QL_CMP_SUB},
		pExprMul)
}

// pExprMul parses multiplication, division, and modulo expressions
func pExprMul(p *Parser, node *QLNode) {
	pExprBinop(p, node,
		[]string{"*", "/", "%"}, []uint32{QL_CMP_MUL, QL_CMP_DIV, QL_CMP_MOD}, pExprUnop)
}

// pExprUnop parses unary expressions
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

// pExprAtom parses atomic expressions (literals, symbols, parenthesized expressions)
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
