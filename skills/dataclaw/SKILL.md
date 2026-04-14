---
name: dataclaw
description: "Query and analyze local data files (Parquet, CSV, JSON, XLSX) using the DataClaw VS Code extension. Use for reading file schemas, counting rows, running SQL joins, filtering data, aggregating, getting column statistics, charting distributions, sampling rows, comparing files, exporting results, querying S3 files, or working with Excel sheets. Invoke via #queryFile, #fileMetadata, #columnStats, #exportResults, #summarize, #chart, #sample, #s3File, #diff, #excelSheet or by asking questions about data files."
argument-hint: "Path to a data file or a data analysis question"
---

# DataClaw: AI Data Analytics Skill

## When to Use

- User asks to read, inspect, or analyze a local data file (`.parquet`, `.csv`, `.json`, `.jsonl`, `.xlsx`)
- User wants to run SQL against local files
- User asks questions like "how many rows", "what columns", "show me the first N rows", "filter where...", "join ... and ..."
- User wants to summarize, aggregate, or explore data without loading it into a separate tool
- User asks about column distributions, histograms, or chart data
- User wants a random sample of rows from a file
- User wants to compare or diff two data files
- User needs to query data files in Amazon S3
- User wants to work with specific sheets in an Excel file
- User wants to export query results to a file

## Available Tools

### `#queryFile` — `dataclaw_QueryFileTool`

Executes SQL against one or more data files. Each file is automatically accessible as a view named by its basename without extension.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePaths` | `string[]` | ✅ | Absolute paths to data files |
| `sql` | `string` | ✅ | SQL query to execute |
| `pageSize` | `number` | — | Max rows returned per statement (default: 100) |

**View naming**: `/data/sales.parquet` → view `sales`, `/tmp/users.csv` → view `users`.  
SQL can reference views by name **or** by raw file path — both work.

---

### `#fileMetadata` — `dataclaw_FileMetadataTool`

Inspect data file schema and metadata without running SQL.

| Command | Description |
|---|---|
| `describeFile` | Returns column names, types, and row count |
| `summarizeFile` | Returns per-column distinct count, null percentage, and inferred type |
| `parquetMetadata` | Returns Parquet footer key-value metadata |

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePath` | `string` | ✅ | Absolute path to the data file |

---

### `#columnStats` — `dataclaw_ColumnStatsTool`

Get detailed statistics for a single column including distributions and histograms.

| Command | Description |
|---|---|
| `fileColumnStats` | Stats from a data file directly |
| `cacheColumnStats` | Stats from a cached query result |

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePath` | `string` | For `fileColumnStats` | Absolute path to the data file |
| `cacheId` | `string` | For `cacheColumnStats` | Cache ID from a previous query result |
| `column` | `string` | ✅ | Column name to analyze |
| `whereClause` | `string` | — | Optional SQL WHERE filter (for `cacheColumnStats`) |

Returns: count, nulls, unique values, min, max. Numeric columns also get mean, stddev, quantiles, histogram. String columns get top values. Date columns get timeseries.

---

### `#exportResults` — `dataclaw_ExportTool`

Export cached query results to a file in CSV, Parquet, JSON, or JSONL format.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `cacheId` | `string` | ✅ | Cache ID from a previous query result |
| `format` | `string` | ✅ | `csv`, `parquet`, `json`, or `jsonl` |
| `outputPath` | `string` | ✅ | Absolute path for the output file |
| `maxRows` | `number` | — | Maximum rows to export (all if omitted) |
| `sortColumn` | `string` | — | Column to sort by before export |
| `sortDirection` | `string` | — | `asc` or `desc` |
| `whereClause` | `string` | — | SQL WHERE clause to filter rows |

---

### `#summarize` — `dataclaw_SummarizeTool`

Quick summary of all columns in a data file or cached query result.

| Command | Description |
|---|---|
| `summarizeFile` | Summarize columns from a data file |
| `summarizeCache` | Summarize columns from a cached query result |

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePath` | `string` | For `summarizeFile` | Absolute path to the data file |
| `cacheId` | `string` | For `summarizeCache` | Cache ID from a previous query result |

Returns per-column: name, distinctCount, nullPercent, inferredType.

---

### `#chart` — `dataclaw_ChartTool`

Get chart-ready data for a column. Returns histogram (numeric), top values (string), or timeseries (date).

| Command | Description |
|---|---|
| `chartFileColumn` | Chart data from a file column |
| `chartCacheColumn` | Chart data from a cached result column |

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePath` | `string` | For `chartFileColumn` | Absolute path to the data file |
| `cacheId` | `string` | For `chartCacheColumn` | Cache ID from a previous query result |
| `column` | `string` | ✅ | Column name to chart |
| `whereClause` | `string` | — | Optional SQL WHERE filter (for `chartCacheColumn`) |

Returns: `chartType` (`histogram`, `bar`, or `timeseries`) plus the corresponding data arrays.

---

### `#sample` — `dataclaw_SampleTool`

Get a random sample of rows using reservoir sampling.

| Command | Description |
|---|---|
| `sampleFile` | Random rows from a data file |
| `sampleCache` | Random rows from a cached query result |

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePath` | `string` | For `sampleFile` | Absolute path to the data file |
| `cacheId` | `string` | For `sampleCache` | Cache ID from a previous query result |
| `rows` | `number` | — | Number of random rows to return (default: 10) |

---

### `#s3File` — `dataclaw_S3FileTool`

Query or describe data files stored in Amazon S3 directly.

| Command | Description |
|---|---|
| `queryS3` | Run SQL against an S3 file |
| `describeS3` | Get schema and row count |

| Parameter | Type | Required | Description |
|---|---|---|---|
| `s3Path` | `string` | ✅ | S3 URI (e.g. `s3://bucket/path/to/file.parquet`) |
| `sql` | `string` | For `queryS3` | SQL query (defaults to `SELECT *` if omitted) |
| `pageSize` | `number` | — | Max rows to return (default: 100) |
| `accessKeyId` | `string` | — | AWS access key ID (uses default credentials if omitted) |
| `secretAccessKey` | `string` | — | AWS secret access key |
| `sessionToken` | `string` | — | AWS session token |
| `region` | `string` | — | AWS region |

---

### `#diff` — `dataclaw_DiffTool`

Compare two data files and find added, removed, or changed rows.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePathA` | `string` | ✅ | Absolute path to the baseline file |
| `filePathB` | `string` | ✅ | Absolute path to the comparison file |
| `mode` | `string` | — | `added`, `removed`, `changed`, or `all` (default: `all`) |
| `keyColumns` | `string[]` | — | Key columns for detecting changed rows |
| `maxRows` | `number` | — | Max diff rows per category (default: 100) |

---

### `#excelSheet` — `dataclaw_ExcelSheetTool`

Work with specific sheets in Excel (.xlsx) files.

| Command | Description |
|---|---|
| `describeSheet` | Column names, types, and row count |
| `summarizeSheet` | Per-column statistics |
| `sheetColumnStats` | Detailed stats for one column |
| `querySheet` | Run SQL against the sheet |

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePath` | `string` | ✅ | Absolute path to the .xlsx file |
| `sheet` | `string` | ✅ | Sheet name |
| `column` | `string` | For `sheetColumnStats` | Column name to analyze |
| `sql` | `string` | For `querySheet` | SQL query (sheet is available as a view) |
| `pageSize` | `number` | — | Max rows to return (default: 100, for `querySheet`) |

## Procedures

### Inspect a File Schema

Ask: "What columns does /path/to/file.parquet have?"

```
filePaths: ["/path/to/file.parquet"]
sql: "DESCRIBE sales"
```

Or equivalently:

```sql
SELECT * FROM '/path/to/file.parquet' LIMIT 0
```

---

### Count Rows

Ask: "How many rows are in /data/orders.csv?"

```
filePaths: ["/data/orders.csv"]
sql: "SELECT COUNT(*) AS total_rows FROM orders"
```

---

### Preview Data

Ask: "Show me the first 10 rows of /data/sales.parquet"

```
filePaths: ["/data/sales.parquet"]
sql: "SELECT * FROM sales LIMIT 10"
pageSize: 10
```

---

### Filter & Aggregate

Ask: "What are the top 5 products by revenue in /data/sales.parquet?"

```
filePaths: ["/data/sales.parquet"]
sql: "SELECT product, SUM(revenue) AS total_revenue FROM sales GROUP BY product ORDER BY total_revenue DESC LIMIT 5"
```

---

### Join Multiple Files

Ask: "Join /data/orders.parquet and /data/customers.csv on customer_id and show revenue by region"

```
filePaths: ["/data/orders.parquet", "/data/customers.csv"]
sql: "SELECT c.region, SUM(o.revenue) AS total FROM orders o JOIN customers c ON o.customer_id = c.customer_id GROUP BY c.region ORDER BY total DESC"
```

---

### Multi-Statement Queries

Multiple semicolon-separated statements are supported. Each statement result is returned separately.

```
filePaths: ["/data/sales.parquet"]
sql: "SELECT COUNT(*) FROM sales; SELECT AVG(revenue) FROM sales; SELECT MAX(revenue) FROM sales"
```

---

### Describe File Structure

Ask: "What's the schema of /data/events.parquet?"

```
command: "describeFile"
params: { filePath: "/data/events.parquet" }
```

Returns `{ columns: [{name, type}], rowCount }`.

---

### Summarize All Columns

Ask: "Give me a summary of /data/users.csv"

```
command: "summarizeFile"
params: { filePath: "/data/users.csv" }
```

Returns per-column `{ name, distinctCount, nullPercent, inferredType }`.

---

### Get Column Distribution

Ask: "What does the age distribution look like in /data/users.parquet?"

```
command: "fileColumnStats"
params: { filePath: "/data/users.parquet", column: "age" }
```

Returns count, min, max, mean, stddev, quantiles, and histogram for numeric columns.

---

### Export Query Results

Ask: "Export the result to CSV"

After a query returns a cacheId:

```
command: "exportResults"
params: { cacheId: "<id>", format: "csv", outputPath: "/tmp/results.csv" }
```

---

### Summarize a File

Ask: "Give me a quick summary of /data/users.parquet"

```
command: "summarizeFile"
params: { filePath: "/data/users.parquet" }
```

Returns per-column `{ name, distinctCount, nullPercent, inferredType }`.

---

### Chart a Column Distribution

Ask: "Show me the revenue distribution in /data/sales.parquet"

```
command: "chartFileColumn"
params: { filePath: "/data/sales.parquet", column: "revenue" }
```

Returns histogram buckets for numeric columns, top values for string columns, or timeseries bins for date columns.

---

### Random Sample

Ask: "Give me 20 random rows from /data/events.parquet"

```
command: "sampleFile"
params: { filePath: "/data/events.parquet", rows: 20 }
```

---

### Query an S3 File

Ask: "Show me the first 10 rows of s3://my-bucket/data/events.parquet"

```
command: "queryS3"
params: { s3Path: "s3://my-bucket/data/events.parquet", sql: "SELECT * FROM 's3://my-bucket/data/events.parquet' LIMIT 10" }
```

---

### Describe an S3 File

Ask: "What columns are in s3://my-bucket/data/events.parquet?"

```
command: "describeS3"
params: { s3Path: "s3://my-bucket/data/events.parquet" }
```

---

### Compare Two Files

Ask: "What's different between /data/v1.parquet and /data/v2.parquet?"

```
command: "diffFiles"
params: { filePathA: "/data/v1.parquet", filePathB: "/data/v2.parquet" }
```

With key columns for detecting changed rows:

```
command: "diffFiles"
params: { filePathA: "/data/v1.parquet", filePathB: "/data/v2.parquet", mode: "changed", keyColumns: ["id"] }
```

---

### Excel Sheet Operations

Ask: "Describe the Sales sheet in /data/report.xlsx"

```
command: "describeSheet"
params: { filePath: "/data/report.xlsx", sheet: "Sales" }
```

Ask: "Sum revenue by product in the Orders sheet of /data/report.xlsx"

```
command: "querySheet"
params: { filePath: "/data/report.xlsx", sheet: "Orders", sql: "SELECT product, SUM(revenue) as total FROM __excel_Orders GROUP BY product ORDER BY total DESC" }
```

---

## Response Shape

The tool returns a JSON object:

```json
{
  "registeredViews": ["sales"],
  "totalExecutionTimeMs": 42,
  "statementCount": 1,
  "statements": [
    {
      "sql": "SELECT ...",
      "columns": ["product", "total_revenue"],
      "columnTypes": ["VARCHAR", "DOUBLE"],
      "totalRows": 1000,
      "returnedRows": 100,
      "hasMore": true,
      "executionTimeMs": 38,
      "rows": [{ "product": "Widget A", "total_revenue": 52000 }, ...]
    }
  ]
}
```

- `hasMore: true` means there are more rows than `pageSize`. Reduce scope with `LIMIT` or increase `pageSize`.
- `columnTypes` uses SQL type names (e.g. `VARCHAR`, `DOUBLE`, `BIGINT`, `DATE`, `TIMESTAMP`).

---

## Tips

- **Always use absolute file paths.** Relative paths will fail.
- **View names are case-sensitive.** `/Data/Sales.parquet` → view `Sales` (capital S).
- **For XLSX files**, use `#excelSheet` to target a specific sheet, or `#queryFile` to query the first sheet by default.
- **Parquet is fastest.** Column-pruning and predicate pushdown apply automatically — SELECT only needed columns for large files.
- **pageSize** only limits rows returned to the AI; the full dataset is queried. Use `LIMIT` in SQL to reduce data scanned.
- **Schema exploration first**: Use `#fileMetadata` (describeFile) or `#summarize` before writing complex queries on unfamiliar files.
- **Chart tool** returns raw data arrays — the AI should render them as markdown tables or describe the distribution in natural language.
- **S3 files**: DuckDB's httpfs extension is used internally. AWS credentials from the environment are used by default; pass explicit credentials only if needed.
- **Diff tool**: For large files, use `maxRows` to limit output. Use `keyColumns` when files share a primary key to detect value changes.
- **Export tool** requires a `cacheId` from a prior `#queryFile` result. Run a query first, then export.
