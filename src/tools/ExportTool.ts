import * as path from 'path';
import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService } from '../common/Duckdb';

export interface ExportResultsParams {
    cacheId: string;
    format: 'csv' | 'parquet' | 'json' | 'jsonl';
    outputPath: string;
    maxRows?: number;
    sortColumn?: string;
    sortDirection?: 'asc' | 'desc';
    whereClause?: string;
}

export interface ExportQueryParams {
    filePaths: string[];
    sql: string;
    format: 'csv' | 'parquet' | 'json' | 'jsonl';
    outputPath: string;
    maxRows?: number;
}

export interface ExportInput extends BaseToolInput {
    command: 'exportResults' | 'exportQuery';
    params: ExportResultsParams | ExportQueryParams;
}

export class ExportTool extends BaseTool<ExportInput> {
    protected readonly toolName = 'ExportTool';

    protected async executeCommand(command: string, params: ExportResultsParams | ExportQueryParams): Promise<any> {
        switch (command) {
            case 'exportResults':
                return this.exportResults(params as ExportResultsParams);
            case 'exportQuery':
                return this.exportQuery(params as ExportQueryParams);
            default:
                throw new Error(`Unknown command: ${command}. Expected one of: exportResults, exportQuery`);
        }
    }

    private async exportResults(params: ExportResultsParams): Promise<any> {
        const { cacheId, format, outputPath, maxRows, sortColumn, sortDirection, whereClause } = params;

        if (!cacheId || !cacheId.trim()) {
            throw new Error('cacheId parameter is required');
        }
        if (!format) {
            throw new Error('format parameter is required');
        }
        if (!outputPath || !outputPath.trim()) {
            throw new Error('outputPath parameter is required');
        }

        const db = getDuckDBService();
        await db.exportCache(cacheId, format, outputPath, maxRows, sortColumn, sortDirection, whereClause);

        return {
            cacheId,
            format,
            outputPath,
            maxRows: maxRows ?? null,
            message: `Successfully exported to ${outputPath}`,
        };
    }

    private async exportQuery(params: ExportQueryParams): Promise<any> {
        const { filePaths, sql, format, outputPath, maxRows } = params;

        if (!filePaths || filePaths.length === 0) {
            throw new Error('filePaths parameter is required');
        }
        if (!sql || !sql.trim()) {
            throw new Error('sql parameter is required');
        }
        if (!format) {
            throw new Error('format parameter is required');
        }
        if (!outputPath || !outputPath.trim()) {
            throw new Error('outputPath parameter is required');
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

        try {
            // Build the inner SQL with optional row limit
            let innerSql = sql;
            if (maxRows) {
                innerSql = `SELECT * FROM (${sql}) LIMIT ${maxRows}`;
            }

            // Build COPY command based on format
            let copyOptions: string;
            switch (format) {
                case 'csv':
                    copyOptions = 'FORMAT CSV, HEADER';
                    break;
                case 'parquet':
                    copyOptions = 'FORMAT PARQUET';
                    break;
                case 'json':
                    copyOptions = 'FORMAT JSON, ARRAY true';
                    break;
                case 'jsonl':
                    copyOptions = 'FORMAT JSON, ARRAY false';
                    break;
                default:
                    throw new Error(`Unsupported format: ${format}`);
            }

            const escapedOutputPath = outputPath.replace(/'/g, "''");
            const copySql = `COPY (${innerSql}) TO '${escapedOutputPath}' (${copyOptions})`;
            await db.run(copySql);

            return {
                format,
                outputPath,
                maxRows: maxRows ?? null,
                message: `Successfully exported to ${outputPath}`,
            };
        } finally {
            // Drop all views created for this call
            for (const viewName of registeredViews) {
                await db.run(`DROP VIEW IF EXISTS "${viewName}"`).catch(() => {});
            }
        }
    }
}
