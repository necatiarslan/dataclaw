import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService } from '../common/Duckdb';

export interface ExcelSheetMetadataParams {
    filePath: string;
    sheet: string;
}

export interface ExcelSheetColumnStatsParams {
    filePath: string;
    sheet: string;
    column: string;
}

export interface ExcelSheetQueryParams {
    filePath: string;
    sheet: string;
    sql?: string;
    pageSize?: number;
}

export interface ExcelSheetInput extends BaseToolInput {
    command: 'describeSheet' | 'summarizeSheet' | 'sheetColumnStats' | 'querySheet';
    params: ExcelSheetMetadataParams | ExcelSheetColumnStatsParams | ExcelSheetQueryParams;
}

export class ExcelSheetTool extends BaseTool<ExcelSheetInput> {
    protected readonly toolName = 'ExcelSheetTool';

    protected async executeCommand(command: string, params: any): Promise<any> {
        const db = getDuckDBService();

        switch (command) {
            case 'describeSheet': {
                const { filePath, sheet } = params as ExcelSheetMetadataParams;
                if (!filePath || !filePath.trim()) { throw new Error('filePath parameter is required'); }
                if (!sheet || !sheet.trim()) { throw new Error('sheet parameter is required'); }
                const metadata = await db.getExcelSheetMetadata(filePath, sheet);
                return { filePath, sheet, columns: metadata.columns, rowCount: metadata.rowCount };
            }

            case 'summarizeSheet': {
                const { filePath, sheet } = params as ExcelSheetMetadataParams;
                if (!filePath || !filePath.trim()) { throw new Error('filePath parameter is required'); }
                if (!sheet || !sheet.trim()) { throw new Error('sheet parameter is required'); }
                const summaries = await db.getExcelSheetSummaries(filePath, sheet);
                return { filePath, sheet, columnCount: summaries.length, columns: summaries };
            }

            case 'sheetColumnStats': {
                const { filePath, sheet, column } = params as ExcelSheetColumnStatsParams;
                if (!filePath || !filePath.trim()) { throw new Error('filePath parameter is required'); }
                if (!sheet || !sheet.trim()) { throw new Error('sheet parameter is required'); }
                if (!column || !column.trim()) { throw new Error('column parameter is required'); }
                const stats = await db.getExcelSheetColumnStats(filePath, sheet, column);
                return { filePath, sheet, ...stats };
            }

            case 'querySheet': {
                const { filePath, sheet, sql, pageSize = 100 } = params as ExcelSheetQueryParams;
                if (!filePath || !filePath.trim()) { throw new Error('filePath parameter is required'); }
                if (!sheet || !sheet.trim()) { throw new Error('sheet parameter is required'); }

                const escapedPath = filePath.replace(/'/g, "''");
                const escapedSheet = sheet.replace(/'/g, "''");
                const viewName = `__excel_${sheet.replace(/[^a-zA-Z0-9_]/g, '_')}`;
                const src = `read_xlsx('${escapedPath}', sheet = '${escapedSheet}', ignore_errors = true)`;

                await db.run(`CREATE OR REPLACE TEMP VIEW "${viewName}" AS SELECT * FROM ${src}`);
                try {
                    const query = sql || `SELECT * FROM "${viewName}" LIMIT ${pageSize}`;
                    const result = await db.executeQuery(query, pageSize);
                    const stmt = result.statements[0];
                    return {
                        filePath,
                        sheet,
                        columns: stmt.meta.columns,
                        columnTypes: stmt.meta.columnTypes,
                        totalRows: stmt.meta.totalRows,
                        returnedRows: stmt.page.rows.length,
                        hasMore: stmt.page.rows.length < stmt.meta.totalRows,
                        executionTimeMs: Math.round(stmt.meta.executionTime),
                        rows: stmt.page.rows,
                    };
                } finally {
                    await db.run(`DROP VIEW IF EXISTS "${viewName}"`).catch(() => {});
                }
            }

            default:
                throw new Error(`Unknown command: ${command}. Expected one of: describeSheet, summarizeSheet, sheetColumnStats, querySheet`);
        }
    }
}
