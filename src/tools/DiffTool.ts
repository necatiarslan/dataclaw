import * as path from 'path';
import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService } from '../common/Duckdb';

export interface DiffParams {
    filePathA: string;
    filePathB: string;
    mode?: 'added' | 'removed' | 'changed' | 'all';
    keyColumns?: string[];
    maxRows?: number;
}

export interface DiffInput extends BaseToolInput {
    command: 'diffFiles';
    params: DiffParams;
}

export class DiffTool extends BaseTool<DiffInput> {
    protected readonly toolName = 'DiffTool';

    protected async executeCommand(command: string, params: DiffParams): Promise<any> {
        if (command !== 'diffFiles') {
            throw new Error(`Unknown command: ${command}. Expected: diffFiles`);
        }

        const { filePathA, filePathB, mode = 'all', keyColumns, maxRows = 100 } = params;

        if (!filePathA || !filePathA.trim()) {
            throw new Error('filePathA parameter is required');
        }
        if (!filePathB || !filePathB.trim()) {
            throw new Error('filePathB parameter is required');
        }

        const db = getDuckDBService();
        const escapedA = filePathA.replace(/'/g, "''");
        const escapedB = filePathB.replace(/'/g, "''");
        const nameA = path.basename(filePathA, path.extname(filePathA));
        const nameB = path.basename(filePathB, path.extname(filePathB));

        // Create temp views
        await db.run(`CREATE OR REPLACE TEMP VIEW "${nameA}" AS SELECT * FROM '${escapedA}'`);
        await db.run(`CREATE OR REPLACE TEMP VIEW "${nameB}" AS SELECT * FROM '${escapedB}'`);

        try {
            const result: any = { filePathA, filePathB, mode };
            const limit = `LIMIT ${maxRows}`;

            if (mode === 'added' || mode === 'all') {
                // Rows in B but not in A
                const addedResult = await db.executeQuery(
                    `SELECT * FROM "${nameB}" EXCEPT SELECT * FROM "${nameA}" ${limit}`,
                    maxRows
                );
                result.added = {
                    count: addedResult.statements[0].meta.totalRows,
                    rows: addedResult.statements[0].page.rows,
                };
            }

            if (mode === 'removed' || mode === 'all') {
                // Rows in A but not in B
                const removedResult = await db.executeQuery(
                    `SELECT * FROM "${nameA}" EXCEPT SELECT * FROM "${nameB}" ${limit}`,
                    maxRows
                );
                result.removed = {
                    count: removedResult.statements[0].meta.totalRows,
                    rows: removedResult.statements[0].page.rows,
                };
            }

            if ((mode === 'changed' || mode === 'all') && keyColumns && keyColumns.length > 0) {
                // Rows with same key but different values
                const joinCondition = keyColumns.map(k => `a."${k}" = b."${k}"`).join(' AND ');
                const keySelect = keyColumns.map(k => `a."${k}"`).join(', ');

                // Get column list from view A
                const descResult = await db.executeQuery(`DESCRIBE SELECT * FROM "${nameA}"`, 1000);
                const allColumns = descResult.statements[0].page.rows.map((r: any) => r.column_name as string);
                const nonKeyColumns = allColumns.filter(c => !keyColumns.includes(c));

                if (nonKeyColumns.length > 0) {
                    const diffConditions = nonKeyColumns.map(c =>
                        `a."${c}" IS DISTINCT FROM b."${c}"`
                    ).join(' OR ');

                    const changedSql = `
                        SELECT ${keySelect},
                        ${nonKeyColumns.map(c => `a."${c}" AS "${c}_old", b."${c}" AS "${c}_new"`).join(', ')}
                        FROM "${nameA}" a
                        JOIN "${nameB}" b ON ${joinCondition}
                        WHERE ${diffConditions}
                        ${limit}
                    `;
                    const changedResult = await db.executeQuery(changedSql, maxRows);
                    result.changed = {
                        count: changedResult.statements[0].meta.totalRows,
                        rows: changedResult.statements[0].page.rows,
                    };
                }
            }

            // Summary counts
            const countA = await db.executeQuery(`SELECT COUNT(*) as cnt FROM "${nameA}"`, 1);
            const countB = await db.executeQuery(`SELECT COUNT(*) as cnt FROM "${nameB}"`, 1);
            result.summary = {
                rowsInA: Number(countA.statements[0].page.rows[0]?.cnt ?? 0),
                rowsInB: Number(countB.statements[0].page.rows[0]?.cnt ?? 0),
            };

            return result;
        } finally {
            await db.run(`DROP VIEW IF EXISTS "${nameA}"`).catch(() => {});
            await db.run(`DROP VIEW IF EXISTS "${nameB}"`).catch(() => {});
        }
    }
}
