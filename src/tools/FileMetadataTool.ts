import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService } from '../common/Duckdb';

export interface FileMetadataParams {
    filePath: string;
}

export interface FileMetadataInput extends BaseToolInput {
    command: 'describeFile' | 'summarizeFile' | 'parquetMetadata';
    params: FileMetadataParams;
}

export class FileMetadataTool extends BaseTool<FileMetadataInput> {
    protected readonly toolName = 'FileMetadataTool';

    protected async executeCommand(command: string, params: FileMetadataParams): Promise<any> {
        const { filePath } = params;

        if (!filePath || !filePath.trim()) {
            throw new Error('filePath parameter is required');
        }

        const db = getDuckDBService();

        switch (command) {
            case 'describeFile': {
                const metadata = await db.getFileMetadata(filePath);
                return {
                    filePath,
                    columns: metadata.columns,
                    rowCount: metadata.rowCount,
                };
            }

            case 'summarizeFile': {
                const summaries = await db.getFileSummaries(filePath);
                return {
                    filePath,
                    columnCount: summaries.length,
                    columns: summaries,
                };
            }

            case 'parquetMetadata': {
                const kvMeta = await db.getParquetKvMetadata(filePath);
                return {
                    filePath,
                    keyValueMetadata: kvMeta,
                };
            }

            default:
                throw new Error(`Unknown command: ${command}. Expected one of: describeFile, summarizeFile, parquetMetadata`);
        }
    }
}
