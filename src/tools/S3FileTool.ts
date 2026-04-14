import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService } from '../common/Duckdb';

export interface S3ReadParams {
    s3Path: string;
    sql?: string;
    pageSize?: number;
    accessKeyId?: string;
    secretAccessKey?: string;
    sessionToken?: string;
    region?: string;
}

export interface S3FileInput extends BaseToolInput {
    command: 'queryS3' | 'describeS3';
    params: S3ReadParams;
}

export class S3FileTool extends BaseTool<S3FileInput> {
    protected readonly toolName = 'S3FileTool';

    protected async executeCommand(command: string, params: S3ReadParams): Promise<any> {
        const { s3Path, sql, pageSize = 100, accessKeyId, secretAccessKey, sessionToken, region } = params;

        if (!s3Path || !s3Path.trim()) {
            throw new Error('s3Path parameter is required (e.g. s3://bucket/key.parquet)');
        }

        const db = getDuckDBService();

        // Configure S3 credentials if provided
        if (accessKeyId && secretAccessKey) {
            await db.run(`SET s3_access_key_id = '${accessKeyId.replace(/'/g, "''")}'`);
            await db.run(`SET s3_secret_access_key = '${secretAccessKey.replace(/'/g, "''")}'`);
            if (sessionToken) {
                await db.run(`SET s3_session_token = '${sessionToken.replace(/'/g, "''")}'`);
            }
        }
        if (region) {
            await db.run(`SET s3_region = '${region.replace(/'/g, "''")}'`);
        }

        const escaped = s3Path.replace(/'/g, "''");

        switch (command) {
            case 'describeS3': {
                const [describeResult, countResult] = await Promise.all([
                    db.executeQuery(`DESCRIBE SELECT * FROM '${escaped}'`, 1000),
                    db.executeQuery(`SELECT COUNT(*) as cnt FROM '${escaped}'`, 1),
                ]);

                const descStmt = describeResult.statements[0];
                const countStmt = countResult.statements[0];
                const rowCount = Number(countStmt.page.rows[0]?.cnt ?? 0);

                return {
                    s3Path,
                    columns: descStmt.page.rows.map((r: any) => ({
                        name: r.column_name,
                        type: r.column_type,
                    })),
                    rowCount,
                };
            }

            case 'queryS3': {
                const query = sql || `SELECT * FROM '${escaped}' LIMIT ${pageSize}`;
                const result = await db.executeQuery(query, pageSize);

                const statements = result.statements.map((s) => ({
                    sql: s.meta.sql,
                    columns: s.meta.columns,
                    columnTypes: s.meta.columnTypes,
                    totalRows: s.meta.totalRows,
                    returnedRows: s.page.rows.length,
                    hasMore: s.page.rows.length < s.meta.totalRows,
                    executionTimeMs: Math.round(s.meta.executionTime),
                    rows: s.page.rows,
                }));

                return {
                    s3Path,
                    totalExecutionTimeMs: Math.round(result.totalExecutionTime),
                    statementCount: statements.length,
                    statements,
                };
            }

            default:
                throw new Error(`Unknown command: ${command}. Expected one of: queryS3, describeS3`);
        }
    }
}
