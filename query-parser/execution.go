package queryparser

import (
	"fmt"
	relationaldb "govetachun/go-mini-db/relationalDB"
	transaction "govetachun/go-mini-db/transaction"
	"govetachun/go-mini-db/utils"
)

func qlCreateTable(req *QLCreateTable, tx *transaction.DBTX) error {
	return tx.TableNew(&req.Def)
}

func qlSelect(req *QLSelect, tx *transaction.DBTX, out []relationaldb.Record) ([]relationaldb.Record, error) {
	// records
	records, err := qlScan(&req.QLScan, tx, out)
	if err != nil {
		return nil, err
	}
	// output
	for _, irec := range records {
		orec := relationaldb.Record{Cols: req.Names}
		for _, node := range req.Output {
			ctx := QLEvalContex{env: irec}
			qlEval(&ctx, node)
			if ctx.err != nil {
				return nil, ctx.err
			}
			orec.Vals = append(orec.Vals, ctx.out)
		}
		out = append(out, orec)
	}
	return out, nil
}

// for evaluating expressions
type QLEvalContex struct {
	env relationaldb.Record // optional row values
	out relationaldb.Value
	err error
}

// evaluate an expression recursively
func qlEval(ctx *QLEvalContex, node QLNode) {
	if ctx.err != nil {
		return
	}
	switch node.Value.Type {
	// refer to a column
	case QL_SYM:
		if v := ctx.env.Get(string(node.Value.Str)); v != nil {
			ctx.out = *v
		} else {
			qlErr(ctx, "unknown column: %s", node.Value.Str)
		}
	// a literal value
	case QL_I64, QL_STR:
		ctx.out = relationaldb.Value{
			Type: node.Value.Type,
			I64:  node.Value.I64,
			Str:  node.Value.Str,
		}
	// unary ops
	case QL_NEG:
		qlEval(ctx, node.Kids[0])
		if ctx.out.Type == TYPE_INT64 {
			ctx.out.I64 = -ctx.out.I64
		} else {
			qlErr(ctx, "QL_NEG type error")
		}
	// more; omitted...
	default:
		panic("not implemented")
	}
}

// execute a query
func qlScan(req *QLScan, tx *transaction.DBTX, out []relationaldb.Record) ([]relationaldb.Record, error) {
	sc := relationaldb.Scanner{}
	err := qlScanInit(req, &sc)
	if err != nil {
		return nil, err
	}
	err = tx.Scan(req.Table, &sc)
	if err != nil {
		return nil, err
	}
	return qlScanRun(req, &sc, out)
}

// create the `Scanner` from the `INDEX BY` clause
func qlScanInit(req *QLScan, sc *relationaldb.Scanner) error {
	if req.Key1.Value.Type == 0 {
		// no `INDEX BY`; scan by the primary key
		sc.Cmp1, sc.Cmp2 = relationaldb.CMP_GE, relationaldb.CMP_LE
		return nil
	}
	var err error
	sc.Key1, sc.Cmp1, err = qlEvalScanKey(req.Key1)
	if err != nil {
		return err
	}
	if req.Key2.Value.Type != 0 {
		sc.Key2, sc.Cmp2, err = qlEvalScanKey(req.Key1)
		if err != nil {
			return err
		}
	}
	if req.Key1.Value.Type == QL_CMP_EQ && req.Key2.Value.Type != 0 {
		return fmt.Errorf("bad `INDEX BY`")
	}
	if req.Key1.Value.Type == QL_CMP_EQ {
		sc.Key2 = sc.Key1
		sc.Cmp1, sc.Cmp2 = relationaldb.CMP_GE, relationaldb.CMP_LE
	}
	return nil
}

// qlErr sets an error in the evaluation context
func qlErr(ctx *QLEvalContex, format string, args ...interface{}) {
	ctx.err = fmt.Errorf(format, args...)
}
func qlEvalScanKey(node QLNode) (relationaldb.Record, int, error) {
	if node.Value.Type == QL_UNINIT {
		return relationaldb.Record{}, 0, nil
	}
	key, cmp, err := qlEvalScanKey(node.Kids[0])
	if err != nil {
		return relationaldb.Record{}, 0, err
	}
	return key, cmp, nil
}

// fetch all rows from a `Scanner`
func qlScanRun(req *QLScan, sc *relationaldb.Scanner, out []relationaldb.Record) ([]relationaldb.Record, error) {
	for i := int64(0); sc.Valid(); i++ {
		// `LIMIT`
		ok := req.Offset <= i && i < req.Limit
		rec := relationaldb.Record{}
		if ok {
			sc.Deref(&rec)
		}
		// `FILTER`
		if ok && req.Filter.Value.Type != 0 {
			ctx := QLEvalContex{env: rec}
			qlEval(&ctx, req.Filter)
			if ctx.err != nil {
				return nil, ctx.err
			}
			if ctx.out.Type != TYPE_INT64 {
				return nil, fmt.Errorf("filter is not of boolean type")
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
func qlDelete(req *QLDelete, tx *transaction.DBTX) (uint64, error) {
	records, err := qlScan(&req.QLScan, tx, nil)
	if err != nil {
		return 0, err
	}
	tdef := tx.GetDB().GetTableDef(req.Table)
	pk := tdef.Cols[:tdef.PKeys]
	for _, rec := range records {
		key := relationaldb.Record{Cols: pk}
		for _, col := range pk {
			key.Vals = append(key.Vals, *rec.Get(col))
		}
		deleted, err := tx.Delete(req.Table, key)
		utils.Assert(err == nil && deleted, "err == nil && deleted") // deleting an existing row
	}
	return uint64(len(records)), nil
}
