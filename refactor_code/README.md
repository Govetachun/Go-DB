# Go Mini DB - Refactored

A clean, well-structured implementation of a mini database system in Go.

## Project Structure

```
refactor_code/
├── cmd/
│   └── server/
│       └── main.go                 # Main application entry point
├── internal/
│   ├── storage/
│   │   ├── btree/
│   │   │   ├── node.go            # B-tree node operations
│   │   │   ├── operations.go      # Insert, delete, search operations
│   │   │   └── iterator.go        # B-tree iteration
│   │   ├── disk/
│   │   │   ├── mmap.go            # Memory mapping operations
│   │   │   ├── page_manager.go    # Page allocation/deallocation
│   │   │   └── file_ops.go        # File operations
│   │   ├── types.go               # Storage types and constants
│   │   └── kv.go                  # Key-value store interface
│   ├── query/
│   │   ├── parser/
│   │   │   ├── lexer.go           # Tokenization
│   │   │   ├── parser.go          # SQL parsing
│   │   │   ├── ast.go             # Abstract syntax tree
│   │   │   └── errors.go          # Parse errors
│   │   ├── executor/
│   │   │   └── executor.go        # Query execution
│   │   └── types.go               # Query-related types
│   ├── database/
│   │   └── types.go               # Database types and operations
│   ├── transaction/
│   │   └── types.go               # Transaction management
│   └── concurrency/
│       └── types.go               # Concurrency control
├── pkg/
│   ├── utils/
│   │   └── utils.go               # Shared utilities
│   └── errors/
│       └── errors.go              # Error definitions
├── go.mod
└── README.md
```

## Features

- **B-tree Storage Engine**: Efficient disk-based B-tree implementation
- **SQL Query Parser**: Supports basic SQL statements (SELECT, INSERT, UPDATE, DELETE, CREATE TABLE)
- **Query Execution**: Expression evaluation and query processing
- **Transaction Support**: Basic transaction management
- **Concurrency Control**: Reader-writer locks and MVCC support
- **Memory Mapping**: Efficient disk I/O using memory mapping
- **Page Management**: Disk page allocation and free list management

## Architecture

### Storage Layer
- **B-tree**: Core storage engine with node operations, CRUD operations, and iteration
- **Disk Management**: Memory mapping, page management, and file operations
- **Key-Value Interface**: Clean abstraction over the storage engine

### Query Layer
- **Parser**: Lexical analysis and SQL parsing with AST generation
- **Executor**: Query execution engine with expression evaluation

### Database Layer
- **Schema Management**: Table definitions and metadata
- **Transaction Management**: ACID transaction support
- **Concurrency Control**: Multi-version concurrency control

## Usage

### Building and Running

```bash
cd refactor_code
go mod tidy
go run cmd/server/main.go
```

### Example Usage

```go
// Initialize database
store := storage.NewKVStore("./my_database.db")
err := store.Open()
if err != nil {
    log.Fatal(err)
}
defer store.Close()

// Key-value operations
err = store.Set([]byte("key1"), []byte("value1"))
value, found := store.Get([]byte("key1"))
deleted, err := store.Del([]byte("key1"))

// SQL operations
stmt, err := parser.Parse([]byte("SELECT * FROM users WHERE age > 18"))
result, err := executor.ExecuteQuery(stmt, tx)
```

## Key Improvements

1. **Clear Separation of Concerns**: Each package has a single responsibility
2. **Better Error Handling**: Centralized error types and handling
3. **Improved Maintainability**: Logical code organization and clear interfaces
4. **Enhanced Testability**: Isolated components with clear dependencies
5. **Scalable Architecture**: Easy to extend with new features
6. **Go Best Practices**: Proper package structure and naming conventions

## Function Name Preservation

All original function names have been preserved for cross-reference:
- `pExprTuple`, `pKeyword`, `pExprOr`, etc. (parsing functions)
- `qlSelect`, `qlDelete`, `qlEval`, etc. (execution functions)
- `treeDelete`, `treeInsert`, `leafDelete`, etc. (B-tree functions)
- `pageGet`, `pageNew`, `pageDel`, etc. (page management functions)

## Development

The codebase is organized to support:
- Easy testing of individual components
- Clear extension points for new features
- Minimal coupling between layers
- Consistent error handling patterns
- Performance optimization opportunities

## Notes

This refactored version maintains all the original functionality while providing a much cleaner, more maintainable codebase that follows Go best practices and modern software architecture principles.

