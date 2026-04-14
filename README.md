# DataClaw: AI Data Analytics 📊

DataClaw is a Visual Studio Code extension that brings AI-assisted data analytics into your chat experience. Use natural language to query and analyze local data files (Parquet, CSV, JSON, XLSX) — without leaving your editor.

## 💬 Chat Modes

- **Agent or Plan Mode**: The extension registers data tools that the AI assistant can invoke based on your requests. Ask questions in plain English and the assistant will call the appropriate tools to query your files, summarize data, or run SQL. Recommended for most use cases.

- **Ask or Edit Mode**: Use the `@DataClaw` prefix in the chat input to issue queries directly. Recommended for quick one-off queries.

## 🔌 MCP Support
- **VS Code**: No MCP setup needed. DataClaw is built into VS Code Chat (Copilot) — just open a chat and ask about your data files.
- **Google Antigravity / Windsurf / Others**: You need a local MCP server to connect these editors with DataClaw. See [README_MCP](README_MCP.md) for setup instructions.

## 🤖 Available Tools

### QueryFileTool (`#queryFile`)
Executes a SQL query against one or more data files.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `filePaths` | `string[]` | ✅ | Absolute paths to data files (Parquet, CSV, JSON, XLSX) |
| `sql` | `string` | ✅ | SQL to execute. Reference files by view name (e.g. `SELECT * FROM sales`) or by path |
| `pageSize` | `number` | — | Max rows to return per statement (default: 100) |

Each file is automatically accessible by its basename without extension. For example `/data/sales.parquet` becomes the view `sales`.

### FileMetadataTool (`#fileMetadata`)
Inspect data file schema and metadata without writing SQL.

| Command | Description |
|---|---|
| `describeFile` | Column names, types, and row count |
| `summarizeFile` | Per-column distinct count, null percentage, and type |
| `parquetMetadata` | Parquet footer key-value metadata |

### ColumnStatsTool (`#columnStats`)
Get detailed statistics for a single column — count, nulls, min, max, mean, stddev, quantiles, histogram, top values, or timeseries depending on column type.

| Command | Description |
|---|---|
| `fileColumnStats` | Stats from a data file directly |
| `cacheColumnStats` | Stats from a cached query result |

### ExportTool (`#exportResults`)
Export cached query results to CSV, Parquet, JSON, or JSONL.

### SummarizeTool (`#summarize`)
Quick summary of all columns in a file or cached query result — distinct count, null percentage, and inferred type per column.

| Command | Description |
|---|---|
| `summarizeFile` | Summarize columns from a data file |
| `summarizeCache` | Summarize columns from a cached query result |

### ChartTool (`#chart`)
Get chart-ready data for a column — histogram for numeric, top values for string, timeseries for date columns.

| Command | Description |
|---|---|
| `chartFileColumn` | Chart data from a file column |
| `chartCacheColumn` | Chart data from a cached result column |

### SampleTool (`#sample`)
Get a random sample of rows using reservoir sampling.

| Command | Description |
|---|---|
| `sampleFile` | Random rows from a data file |
| `sampleCache` | Random rows from a cached query result |

### S3FileTool (`#s3File`)
Query or describe data files stored in Amazon S3 directly — no download needed.

| Command | Description |
|---|---|
| `queryS3` | Run SQL against an S3 file |
| `describeS3` | Get schema and row count of an S3 file |

### DiffTool (`#diff`)
Compare two data files and find added, removed, or changed rows.

| Command | Description |
|---|---|
| `diffFiles` | Find differences between two data files |

Supports `mode`: `added`, `removed`, `changed`, or `all`. Use `keyColumns` to detect value changes on matching rows.

### ExcelSheetTool (`#excelSheet`)
Work with specific sheets in Excel (.xlsx) files.

| Command | Description |
|---|---|
| `describeSheet` | Column names, types, and row count for a sheet |
| `summarizeSheet` | Per-column statistics for a sheet |
| `sheetColumnStats` | Detailed stats for one column in a sheet |
| `querySheet` | Run SQL against a specific sheet |

**Example prompts:**
- "Show me the first 10 rows of /Users/me/data/orders.parquet"
- "How many records are in /tmp/users.csv?"
- "Join /data/orders.parquet and /data/customers.csv on customer_id and show revenue by region"
- "What are the top 5 products by sales in /data/sales.parquet?"
- "Summarize all columns in /data/events.parquet"
- "Show me the distribution of the age column in /data/users.csv"
- "Give me a random sample of 20 rows from /data/logs.parquet"
- "Compare /data/v1.parquet and /data/v2.parquet and show what changed"
- "Describe the Sales sheet in /data/report.xlsx"
- "Query s3://my-bucket/data/events.parquet and show the first 10 rows"
- "Export the results to /tmp/output.csv"

## ❓ Q & A

### Data & Privacy
- **Q**: Does DataClaw send my data files to the cloud?  
  **A**: No. All query execution happens locally inside the extension. Only the query results (up to `pageSize` rows) are passed to the AI model as context.

- **Q**: What file formats are supported?  
  **A**: Parquet, CSV, JSON/JSONL, and XLSX.

- **Q**: Can I query multiple files at once?  
  **A**: Yes. Pass multiple paths in `filePaths` and reference them by view name in your SQL query.

### Performance
- **Q**: How large a file can I query?  
  **A**: Files much larger than available RAM are supported through out-of-core processing. Memory is capped at ~1.5 GB within the extension host, with spill-to-disk enabled automatically.

- **Q**: Will querying large Parquet files be slow?  
  **A**: Parquet queries are typically fast — only the columns and row groups needed for your query are read (predicate and projection pushdown).

### Safety
- **Q**: Can DataClaw modify my data files?  
  **A**: No. Files are opened read-only. No write operations are performed on source files.

## ⚙️ Prerequisites

- VS Code 1.104 or later (for Language Model Tools support)
- Data files in a supported format: Parquet, CSV, JSON, or XLSX

## 🏁 Quick Start

1. **Install** the DataClaw extension from the VS Code Marketplace.
2. **Open Chat** in Agent mode.
3. **Ask a question**, for example:
   - "Show me the schema of /Users/me/data/sales.parquet"
   - "How many rows are in /tmp/orders.csv?"
   - "What are the top 10 customers by revenue in /data/sales.parquet?"
4. **Review results**: The assistant will call `QueryFileTool`, execute the SQL locally, and stream the results back.
5. **MCP (non-VS Code editors)**: Start the MCP server from the Command Palette → "Data Claw: Start MCP Server", then configure your editor to connect. See [README_MCP](README_MCP.md).

## 🛠️ Commands

| Command | Description |
|---|---|
| Data Claw: Ask Dataclaw | Open the DataClaw chat participant |
| Data Claw: Command History | View history of tool calls and results |
| Data Claw: Start MCP Server | Start the local MCP bridge server (non-VS Code editors) |
| Data Claw: Stop MCP Servers | Stop all active MCP sessions |
| Data Claw: MCP Management | View and manage MCP server settings |

## 📋 TODO
- Add more data tools and file format support.
- Improve natural language understanding for data queries.
- Settings panel for better user experience.

## 💖 Links

- **Issues & Feature Requests**: https://github.com/necatiarslan/dataclaw/issues
- **Sponsor**: https://github.com/sponsors/necatiarslan
- **License**: MIT
