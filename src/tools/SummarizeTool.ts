import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService } from '../common/Duckdb';

export interface SummarizeFileParams {
    filePath: string;
}

export interface SummarizeCacheParams {
    cacheId: string;
}

export interface SummarizeInput extends BaseToolInput {
    command: 'summarizeFile' | 'summarizeCache';
    params: SummarizeFileParams | SummarizeCacheParams;
}

export class SummarizeTool extends BaseTool<SummarizeInput> {
    protected readonly toolName = 'SummarizeTool';

    protected async executeCommand(command: string, params: SummarizeFileParams | SummarizeCacheParams): Promise<any> {
        const db = getDuckDBService();

        switch (command) {
            case 'summarizeFile': {
                const p = params as SummarizeFileParams;
                if (!p.filePath || !p.filePath.trim()) {
                    throw new Error('filePath parameter is required');
                }
                const summaries = await db.getFileSummaries(p.filePath);
                return {
                    filePath: p.filePath,
                    columnCount: summaries.length,
                    columns: summaries,
                };
            }

            case 'summarizeCache': {
                const p = params as SummarizeCacheParams;
                if (!p.cacheId || !p.cacheId.trim()) {
                    throw new Error('cacheId parameter is required');
                }
                const summaries = await db.getCacheColumnSummaries(p.cacheId);
                return {
                    cacheId: p.cacheId,
                    columnCount: summaries.length,
                    columns: summaries,
                };
            }

            default:
                throw new Error(`Unknown command: ${command}. Expected one of: summarizeFile, summarizeCache`);
        }
    }
}
