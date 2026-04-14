import * as path from 'path';
import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService } from '../common/Duckdb';

export interface SampleFileParams {
    filePath: string;
    rows?: number;
}

export interface SampleCacheParams {
    cacheId: string;
    rows?: number;
}

export interface SampleInput extends BaseToolInput {
    command: 'sampleFile' | 'sampleCache';
    params: SampleFileParams | SampleCacheParams;
}

export class SampleTool extends BaseTool<SampleInput> {
    protected readonly toolName = 'SampleTool';

    protected async executeCommand(command: string, params: SampleFileParams | SampleCacheParams): Promise<any> {
        const db = getDuckDBService();
        const limit = (params as any).rows ?? 10;

        switch (command) {
            case 'sampleFile': {
                const p = params as SampleFileParams;
                if (!p.filePath || !p.filePath.trim()) {
                    throw new Error('filePath parameter is required');
                }
                const escaped = p.filePath.replace(/'/g, "''");
                const basename = path.basename(p.filePath, path.extname(p.filePath));

                // Create temp view, sample, then drop
                await db.run(`CREATE OR REPLACE TEMP VIEW "${basename}" AS SELECT * FROM '${escaped}'`);
                try {
                    const result = await db.executeQuery(
                        `SELECT * FROM "${basename}" USING SAMPLE ${limit} ROWS`,
                        limit
                    );
                    const stmt = result.statements[0];
                    return {
                        filePath: p.filePath,
                        columns: stmt.meta.columns,
                        columnTypes: stmt.meta.columnTypes,
                        sampledRows: stmt.page.rows.length,
                        totalRows: stmt.meta.totalRows,
                        rows: stmt.page.rows,
                    };
                } finally {
                    await db.run(`DROP VIEW IF EXISTS "${basename}"`).catch(() => {});
                }
            }

            case 'sampleCache': {
                const p = params as SampleCacheParams;
                if (!p.cacheId || !p.cacheId.trim()) {
                    throw new Error('cacheId parameter is required');
                }
                const result = await db.executeQuery(
                    `SELECT * FROM "${p.cacheId}" USING SAMPLE ${limit} ROWS`,
                    limit
                );
                const stmt = result.statements[0];
                return {
                    cacheId: p.cacheId,
                    columns: stmt.meta.columns,
                    columnTypes: stmt.meta.columnTypes,
                    sampledRows: stmt.page.rows.length,
                    totalRows: stmt.meta.totalRows,
                    rows: stmt.page.rows,
                };
            }

            default:
                throw new Error(`Unknown command: ${command}. Expected one of: sampleFile, sampleCache`);
        }
    }
}
