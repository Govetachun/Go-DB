package parser

import (
	"fmt"
	"govetachun/go-mini-db/refactor_code/internal/query"
)

// Re-export types from query package
type QLNode = query.QLNode
type QLSelect = query.QLSelect
type QLInsert = query.QLInsert
type QLUpdate = query.QLUpdate
type QLDelete = query.QLDelete
type QLCreateTable = query.QLCreateTable
type QLScan = query.QLScan
type Parser = query.Parser
type Value = query.Value

// Re-export constants
const (
	QL_I64     = query.QL_I64
	QL_STR     = query.QL_STR
	QL_SYM     = query.QL_SYM
	QL_TUP     = query.QL_TUP
	QL_ERR     = query.QL_ERR
	QL_NOT     = query.QL_NOT
	QL_NEG     = query.QL_NEG
	QL_CMP_OR  = query.QL_CMP_OR
	QL_CMP_AND = query.QL_CMP_AND
	QL_CMP_LE  = query.QL_CMP_LE
	QL_CMP_GE  = query.QL_CMP_GE
	QL_CMP_LT  = query.QL_CMP_LT
	QL_CMP_GT  = query.QL_CMP_GT
	QL_CMP_EQ  = query.QL_CMP_EQ
	QL_CMP_NE  = query.QL_CMP_NE
	QL_CMP_ADD = query.QL_CMP_ADD
	QL_CMP_SUB = query.QL_CMP_SUB
	QL_CMP_MUL = query.QL_CMP_MUL
	QL_CMP_DIV = query.QL_CMP_DIV
	QL_CMP_MOD = query.QL_CMP_MOD

	MODE_INSERT_ONLY = query.MODE_INSERT_ONLY
	MODE_UPDATE_ONLY = query.MODE_UPDATE_ONLY
	MODE_UPSERT      = query.MODE_UPSERT
)

// pErr sets an error in the parser
func pErr(p *Parser, node *QLNode, msg string) {
	if node != nil {
		node.Value.Type = QL_ERR
		node.Value.Str = []byte(msg)
	}
	p.Err = fmt.Errorf("parse error: %s", msg)
}

// pStmt parses a SQL statement
func pStmt(p *Parser) interface{} {
	switch {
	case pKeyword(p, "create", "table"):
		return pQLCreateTable(p)
	case pKeyword(p, "select"):
		return pQLSelect(p)
	case pKeyword(p, "insert", "into"):
		return pQLInsert(p, MODE_INSERT_ONLY)
	case pKeyword(p, "replace", "into"):
		return pQLInsert(p, MODE_UPDATE_ONLY)
	case pKeyword(p, "upsert", "into"):
		return pQLInsert(p, MODE_UPSERT)
	case pKeyword(p, "delete", "from"):
		return pQLDelete(p)
	case pKeyword(p, "update"):
		return pQLUpdate(p)
	default:
		pErr(p, nil, "unknown stmt")
		return nil
	}
}

// pQLSelect parses a SELECT statement
func pQLSelect(p *Parser) *QLSelect {
	stmt := QLSelect{}
	// SELECT xxx
	pQLSelectExprList(p, &stmt)
	// FROM table
	if !pKeyword(p, "from") {
		pErr(p, nil, "expect `FROM` table")
	}
	stmt.Table = pQLMustSym(p)
	// INDEX BY xxx FILTER yyy LIMIT zzz
	pQLScan(p, &stmt.QLScan)
	if p.Err != nil {
		return nil
	}
	return &stmt
}

func pQLSelectExprList(p *Parser, stmt *QLSelect) {
	pQLSelectExpr(p, stmt)
	for pKeyword(p, ",") {
		pQLSelectExpr(p, stmt)
	}
}

func pQLSelectExpr(p *Parser, stmt *QLSelect) {
	expr := QLNode{}
	pExprOr(p, &expr)
	name := ""
	if pKeyword(p, "as") {
		name = pQLMustSym(p)
	}
	stmt.Names = append(stmt.Names, name)
	stmt.Output = append(stmt.Output, expr)
}

// pQLScan parses common scan clauses (INDEX BY, FILTER, LIMIT)
func pQLScan(p *Parser, scan *QLScan) {
	// INDEX BY xxx
	if pKeyword(p, "index", "by") {
		// Parse index key
		scan.Key1 = QLNode{}
		pExprOr(p, &scan.Key1)
	}

	// FILTER xxx
	if pKeyword(p, "filter") {
		scan.Filter = QLNode{}
		pExprOr(p, &scan.Filter)
	}

	// LIMIT x, y
	if pKeyword(p, "limit") {
		// Parse limit and offset
		limitNode := QLNode{}
		if pNum(p, &limitNode) {
			scan.Limit = limitNode.Value.I64
		}

		if pKeyword(p, ",") {
			offsetNode := QLNode{}
			if pNum(p, &offsetNode) {
				scan.Offset = offsetNode.Value.I64
			}
		}
	}
}

// pQLCreateTable parses a CREATE TABLE statement
func pQLCreateTable(p *Parser) *QLCreateTable {
	stmt := QLCreateTable{}
	// Parse table definition
	// This is a simplified implementation
	return &stmt
}

// pQLInsert parses an INSERT/REPLACE/UPSERT statement
func pQLInsert(p *Parser, mode int) *QLInsert {
	stmt := QLInsert{Mode: mode}

	// Parse table name
	stmt.Table = pQLMustSym(p)

	// Parse column names
	if pKeyword(p, "(") {
		for {
			col := pQLMustSym(p)
			stmt.Names = append(stmt.Names, col)
			if !pKeyword(p, ",") {
				break
			}
		}
		if !pKeyword(p, ")") {
			pErr(p, nil, "expect ')'")
		}
	}

	// Parse VALUES
	if pKeyword(p, "values") {
		for {
			if pKeyword(p, "(") {
				row := []QLNode{}
				for {
					expr := QLNode{}
					pExprOr(p, &expr)
					row = append(row, expr)
					if !pKeyword(p, ",") {
						break
					}
				}
				stmt.Values = append(stmt.Values, row)
				if !pKeyword(p, ")") {
					pErr(p, nil, "expect ')'")
				}
			}
			if !pKeyword(p, ",") {
				break
			}
		}
	}

	return &stmt
}

// pQLDelete parses a DELETE statement
func pQLDelete(p *Parser) *QLDelete {
	stmt := QLDelete{}

	// Parse table name
	stmt.Table = pQLMustSym(p)

	// Parse scan clauses
	pQLScan(p, &stmt.QLScan)

	return &stmt
}

// pQLUpdate parses an UPDATE statement
func pQLUpdate(p *Parser) *QLUpdate {
	stmt := QLUpdate{}

	// Parse table name
	stmt.Table = pQLMustSym(p)

	// Parse SET clause
	if pKeyword(p, "set") {
		for {
			col := pQLMustSym(p)
			stmt.Names = append(stmt.Names, col)

			if !pKeyword(p, "=") {
				pErr(p, nil, "expect '='")
			}

			expr := QLNode{}
			pExprOr(p, &expr)
			stmt.Values = append(stmt.Values, expr)

			if !pKeyword(p, ",") {
				break
			}
		}
	}

	// Parse scan clauses
	pQLScan(p, &stmt.QLScan)

	return &stmt
}

// Parse creates a new parser and parses the input
func Parse(input []byte) (interface{}, error) {
	p := &Parser{Input: input, Idx: 0}
	stmt := pStmt(p)
	if p.Err != nil {
		return nil, p.Err
	}
	return stmt, nil
}
