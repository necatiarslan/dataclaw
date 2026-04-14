import { BaseTool, BaseToolInput } from '../common/BaseTool';
import { getDuckDBService, ColumnStats } from '../common/Duckdb';

export interface ChartFileParams {
    filePath: string;
    column: string;
}

export interface ChartCacheParams {
    cacheId: string;
    column: string;
    whereClause?: string;
}

export interface ChartInput extends BaseToolInput {
    command: 'chartFileColumn' | 'chartCacheColumn';
    params: ChartFileParams | ChartCacheParams;
}

export class ChartTool extends BaseTool<ChartInput> {
    protected readonly toolName = 'ChartTool';

    protected async executeCommand(command: string, params: ChartFileParams | ChartCacheParams): Promise<any> {
        const db = getDuckDBService();

        let stats: ColumnStats;

        switch (command) {
            case 'chartFileColumn': {
                const p = params as ChartFileParams;
                if (!p.filePath || !p.filePath.trim()) {
                    throw new Error('filePath parameter is required');
                }
                if (!p.column || !p.column.trim()) {
                    throw new Error('column parameter is required');
                }
                stats = await db.getFileColumnStats(p.filePath, p.column);
                break;
            }

            case 'chartCacheColumn': {
                const p = params as ChartCacheParams;
                if (!p.cacheId || !p.cacheId.trim()) {
                    throw new Error('cacheId parameter is required');
                }
                if (!p.column || !p.column.trim()) {
                    throw new Error('column parameter is required');
                }
                stats = await db.getCacheColumnStats(p.cacheId, p.column, p.whereClause);
                break;
            }

            default:
                throw new Error(`Unknown command: ${command}. Expected one of: chartFileColumn, chartCacheColumn`);
        }

        // Build chart-oriented response based on column type
        const chart: any = {
            column: stats.column,
            type: stats.type,
            total: stats.total,
            nonNull: stats.nonNull,
            nullCount: stats.nullCount,
            unique: stats.unique,
            min: stats.min,
            max: stats.max,
        };

        if (stats.type === 'numeric') {
            chart.mean = stats.mean;
            chart.stddev = stats.stddev;
            chart.quantiles = stats.quantiles;
            chart.histogram = stats.histogram;
            chart.chartType = 'histogram';
        } else if (stats.type === 'date' && stats.timeseries) {
            chart.timeseries = stats.timeseries;
            chart.chartType = 'timeseries';
        } else if (stats.type === 'string' && stats.topValues) {
            chart.topValues = stats.topValues;
            chart.chartType = 'bar';
        }

        return chart;
    }
}
