import * as path from 'path';
import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService, MultiQueryResultWithPages } from '../common/Duckdb';

export interface QueryFileParams {
    filePaths: string[];
    sql: string;
    pageSize?: number;
}

export interface QueryFileInput extends BaseToolInput {
    command: 'queryFile';
    params: QueryFileParams;
}

export class QueryFileTool extends BaseTool<QueryFileInput> {
    protected readonly toolName = 'QueryFileTool';

    protected async executeCommand(command: string, params: QueryFileParams): Promise<any> {
        const { filePaths, sql, pageSize = 100 } = params;

        if (!sql || !sql.trim()) {
            throw new Error('sql parameter is required');
        }

        const db = getDuckDBService();
        const registeredViews: string[] = [];

        // Register each file as a temp view named by its basename (without extension)
        for (const filePath of filePaths) {
            const basename = path.basename(filePath, path.extname(filePath));
            const escapedPath = filePath.replace(/'/g, "''");
            await db.run(
                `CREATE OR REPLACE TEMP VIEW "${basename}" AS SELECT * FROM '${escapedPath}'`
            );
            registeredViews.push(basename);
        }

        let result: MultiQueryResultWithPages;
        try {
            result = await db.executeQuery(sql, pageSize);
        } finally {
            // Drop all views created for this call
            for (const viewName of registeredViews) {
                await db.run(`DROP VIEW IF EXISTS "${viewName}"`).catch(() => {});
            }
        }

        // Build a compact, AI-friendly response
        const statements = result.statements.map((s) => {
            const { meta, page } = s;
            return {
                cacheId: meta.cacheId,
                sql: meta.sql,
                columns: meta.columns,
                columnTypes: meta.columnTypes,
                totalRows: meta.totalRows,
                returnedRows: page.rows.length,
                hasMore: page.rows.length < meta.totalRows,
                executionTimeMs: Math.round(meta.executionTime),
                rows: page.rows,
            };
        });

        return {
            registeredViews,
            totalExecutionTimeMs: Math.round(result.totalExecutionTime),
            statementCount: statements.length,
            statements,
        };
    }
}
