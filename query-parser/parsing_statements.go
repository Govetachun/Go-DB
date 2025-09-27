package queryparser

import "strings"

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
	if p.err != nil {
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

func pQLMustSym(p *Parser) string {
	p.skipSpace()
	end := p.idx
	if !(end < len(p.input) && isSymStart(p.input[end])) {
		pErr(p, nil, "expect symbol")
		return ""
	}
	end++
	for end < len(p.input) && isSym(p.input[end]) {
		end++
	}
	if pKeywordSet[strings.ToLower(string(p.input[p.idx:end]))] {
		pErr(p, nil, "keyword not allowed as symbol")
		return ""
	}
	sym := string(p.input[p.idx:end])
	p.idx = end
	return sym
}

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

func pQLCreateTable(p *Parser) *QLCreateTable {
	stmt := QLCreateTable{}
	// Parse table definition
	// This is a simplified implementation
	return &stmt
}

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

func pQLDelete(p *Parser) *QLDelete {
	stmt := QLDelete{}

	// Parse table name
	stmt.Table = pQLMustSym(p)

	// Parse scan clauses
	pQLScan(p, &stmt.QLScan)

	return &stmt
}

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
