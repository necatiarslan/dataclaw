import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService } from '../common/Duckdb';

export interface ExportParams {
    cacheId: string;
    format: 'csv' | 'parquet' | 'json' | 'jsonl';
    outputPath: string;
    maxRows?: number;
    sortColumn?: string;
    sortDirection?: 'asc' | 'desc';
    whereClause?: string;
}

export interface ExportInput extends BaseToolInput {
    command: 'exportResults';
    params: ExportParams;
}

export class ExportTool extends BaseTool<ExportInput> {
    protected readonly toolName = 'ExportTool';

    protected async executeCommand(command: string, params: ExportParams): Promise<any> {
        if (command !== 'exportResults') {
            throw new Error(`Unknown command: ${command}. Expected: exportResults`);
        }

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
}
