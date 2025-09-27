package executor

import (
	"fmt"
)

// ExecuteSelect executes a SELECT statement
func ExecuteSelect(req *QLSelect, tx DBTX) ([]Record, error) {
	// Initialize output records slice
	var out []Record

	// Execute the scan to get input records
	records, err := qlScan(&req.QLScan, tx, nil)
	if err != nil {
		return nil, fmt.Errorf("scan failed: %w", err)
	}

	// Process each input record
	for _, irec := range records {
		// Create output record with specified column names
		orec := Record{Cols: req.Names}

		// Evaluate each output expression
		for _, node := range req.Output {
			ctx := QLEvalContex{env: irec}
			qlEval(&ctx, node)
			if ctx.err != nil {
				return nil, fmt.Errorf("expression evaluation failed: %w", ctx.err)
			}
			orec.Vals = append(orec.Vals, ctx.out)
		}

		out = append(out, orec)
	}

	return out, nil
}

// qlScan executes a table scan with conditions
func qlScan(req *QLScan, tx DBTX, out []Record) ([]Record, error) {
	sc := Scanner{}
	err := qlScanInit(req, &sc)
	if err != nil {
		return nil, fmt.Errorf("scan initialization failed: %w", err)
	}

	err = tx.Scan(req.Table, &sc)
	if err != nil {
		return nil, fmt.Errorf("table scan failed: %w", err)
	}

	return qlScanRun(req, &sc, out)
}

// qlScanInit initializes the scanner with scan conditions
func qlScanInit(req *QLScan, sc *Scanner) error {
	if req.Key1.Value.Type == 0 {
		// No INDEX BY clause; scan by primary key
		sc.Cmp1, sc.Cmp2 = CMP_GE, CMP_LE
		return nil
	}

	var err error
	sc.Key1, sc.Cmp1, err = qlEvalScanKey(req.Key1)
	if err != nil {
		return fmt.Errorf("key1 evaluation failed: %w", err)
	}

	if req.Key2.Value.Type != 0 {
		sc.Key2, sc.Cmp2, err = qlEvalScanKey(req.Key2)
		if err != nil {
			return fmt.Errorf("key2 evaluation failed: %w", err)
		}
	}

	// Validate scan key configuration
	if req.Key1.Value.Type == QL_CMP_EQ && req.Key2.Value.Type != 0 {
		return fmt.Errorf("bad INDEX BY: cannot use equality with range")
	}

	if req.Key1.Value.Type == QL_CMP_EQ {
		sc.Key2 = sc.Key1
		sc.Cmp1, sc.Cmp2 = CMP_GE, CMP_LE
	}

	return nil
}

// qlEvalScanKey evaluates scan key expressions
func qlEvalScanKey(node QLNode) (Record, int, error) {
	if node.Value.Type == QL_UNINIT {
		return Record{}, 0, nil
	}

	// For now, simplified implementation
	// In a full implementation, this would evaluate complex expressions
	key, cmp, err := qlEvalScanKey(node.Kids[0])
	if err != nil {
		return Record{}, 0, err
	}

	return key, cmp, nil
}

// qlScanRun processes scan results with filters and limits
func qlScanRun(req *QLScan, sc *Scanner, out []Record) ([]Record, error) {
	for i := int64(0); sc.Valid(); i++ {
		// Apply LIMIT constraints
		ok := req.Offset <= i && i < req.Limit

		var rec Record
		if ok {
			sc.Deref(&rec)
		}

		// Apply FILTER conditions
		if ok && req.Filter.Value.Type != 0 {
			ctx := QLEvalContex{env: rec}
			qlEval(&ctx, req.Filter)
			if ctx.err != nil {
				return nil, fmt.Errorf("filter evaluation failed: %w", ctx.err)
			}
			if ctx.out.Type != TYPE_INT64 {
				return nil, fmt.Errorf("filter must be boolean type")
			}
			ok = (ctx.out.I64 != 0)
		}

		if ok {
			out = append(out, rec)
		}

		sc.Next()
	}

	return out, nil
}

// qlEval evaluates expressions recursively
func qlEval(ctx *QLEvalContex, node QLNode) {
	if ctx.err != nil {
		return
	}

	switch node.Value.Type {
	// Column reference
	case QL_SYM:
		if v := ctx.env.Get(string(node.Value.Str)); v != nil {
			ctx.out = *v
		} else {
			qlErr(ctx, "unknown column: %s", node.Value.Str)
		}

	// Literal values
	case QL_I64:
		ctx.out = Value{Type: TYPE_INT64, I64: node.Value.I64}
	case QL_STR:
		ctx.out = Value{Type: TYPE_BYTES, Str: node.Value.Str}

	// Unary operations
	case QL_NEG:
		qlEval(ctx, node.Kids[0])
		if ctx.err != nil {
			return
		}
		if ctx.out.Type == TYPE_INT64 {
			ctx.out.I64 = -ctx.out.I64
		} else {
			qlErr(ctx, "negation requires integer type")
		}

	// Binary operations
	case QL_CMP_EQ:
		qlEvalBinaryOp(ctx, node, func(a, b Value) Value {
			if a.Type != b.Type {
				return Value{Type: TYPE_ERROR, Str: []byte("type mismatch")}
			}
			var result int64
			if a.Type == TYPE_INT64 {
				if a.I64 == b.I64 {
					result = 1
				}
			} else if a.Type == TYPE_BYTES {
				if string(a.Str) == string(b.Str) {
					result = 1
				}
			}
			return Value{Type: TYPE_INT64, I64: result}
		})

	default:
		qlErr(ctx, "unsupported expression type: %d", node.Value.Type)
	}
}

// qlEvalBinaryOp evaluates binary operations
func qlEvalBinaryOp(ctx *QLEvalContex, node QLNode, op func(Value, Value) Value) {
	if len(node.Kids) != 2 {
		qlErr(ctx, "binary operation requires exactly 2 operands")
		return
	}

	// Evaluate left operand
	leftCtx := QLEvalContex{env: ctx.env}
	qlEval(&leftCtx, node.Kids[0])
	if leftCtx.err != nil {
		ctx.err = leftCtx.err
		return
	}

	// Evaluate right operand
	rightCtx := QLEvalContex{env: ctx.env}
	qlEval(&rightCtx, node.Kids[1])
	if rightCtx.err != nil {
		ctx.err = rightCtx.err
		return
	}

	// Apply operation
	ctx.out = op(leftCtx.out, rightCtx.out)
	if ctx.out.Type == TYPE_ERROR {
		qlErr(ctx, string(ctx.out.Str))
	}
}

// qlErr sets an error in the evaluation context
func qlErr(ctx *QLEvalContex, format string, args ...interface{}) {
	ctx.err = fmt.Errorf(format, args...)
}
