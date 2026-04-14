import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService } from '../common/Duckdb';

export interface FileColumnStatsParams {
    filePath: string;
    column: string;
}

export interface CacheColumnStatsParams {
    cacheId: string;
    column: string;
    whereClause?: string;
}

export interface ColumnStatsInput extends BaseToolInput {
    command: 'fileColumnStats' | 'cacheColumnStats';
    params: FileColumnStatsParams | CacheColumnStatsParams;
}

export class ColumnStatsTool extends BaseTool<ColumnStatsInput> {
    protected readonly toolName = 'ColumnStatsTool';

    protected async executeCommand(command: string, params: FileColumnStatsParams | CacheColumnStatsParams): Promise<any> {
        const db = getDuckDBService();

        switch (command) {
            case 'fileColumnStats': {
                const p = params as FileColumnStatsParams;
                if (!p.filePath || !p.filePath.trim()) {
                    throw new Error('filePath parameter is required');
                }
                if (!p.column || !p.column.trim()) {
                    throw new Error('column parameter is required');
                }
                const stats = await db.getFileColumnStats(p.filePath, p.column);
                return { filePath: p.filePath, ...stats };
            }

            case 'cacheColumnStats': {
                const p = params as CacheColumnStatsParams;
                if (!p.cacheId || !p.cacheId.trim()) {
                    throw new Error('cacheId parameter is required');
                }
                if (!p.column || !p.column.trim()) {
                    throw new Error('column parameter is required');
                }
                const stats = await db.getCacheColumnStats(p.cacheId, p.column, p.whereClause);
                return { cacheId: p.cacheId, ...stats };
            }

            default:
                throw new Error(`Unknown command: ${command}. Expected one of: fileColumnStats, cacheColumnStats`);
        }
    }
}
