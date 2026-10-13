import React from 'react';

import {
    Alert,
    Box,
    Button,
    Checkbox,
    Chip,
    Dialog,
    DialogActions,
    DialogContent,
    DialogContentText,
    DialogTitle,
    FormControlLabel,
    IconButton,
    InputAdornment,
    LinearProgress,
    MenuItem,
    Paper,
    Select,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    TableSortLabel,
    TextField,
    Tooltip,
    Typography,
} from '@mui/material';
import { Close, DeleteSweep, Refresh, Warning } from '@mui/icons-material';

import { ConfigGeneric, type ConfigGenericProps, type ConfigGenericState } from '@iobroker/json-config';
import { I18n } from '@iobroker/gui-components';

type DatapointStatus = 'active' | 'loggingDisabled' | 'objectMissing';

type DatapointStat = {
    id: string;
    type: 'Number' | 'String' | 'Boolean' | null;
    count: number;
    firstTs: number | null;
    lastTs: number | null;
    /** size of the files on the disk in bytes */
    bytes: number;
    /** number of day files */
    files: number;
    status: DatapointStatus;
};

type Summary = {
    datapoints: number;
    values: number;
    bytes: number;
    byStatus: Record<DatapointStatus, { datapoints: number; values: number; bytes: number }>;
};

type CleanupPreview = {
    datapoints: number;
    values: number;
    bytes: number;
    items: DatapointStat[];
};

type SortField = 'id' | 'count' | 'bytes' | 'files' | 'lastTs' | 'status';

/** How far back the statistics look. Counting reads every file, so a huge storage needs a way out */
type Range = 'all' | 'year' | 'month';

interface StatisticsState extends ConfigGenericState {
    stats: DatapointStat[] | null;
    summary: Summary | null;
    loading: boolean;
    errorText: string;
    filter: string;
    range: Range;
    sortField: SortField;
    sortAsc: boolean;
    /** the dry-run result, which is what the confirmation dialog shows */
    preview: CleanupPreview | null;
    /** whether the pending cleanup should also drop datapoints that merely have logging switched off */
    includeDisabled: boolean;
    deleting: boolean;
    doneText: string;
}

const RANGE_MS: Record<Range, number | null> = {
    all: null,
    year: 365 * 24 * 3600_000,
    month: 30 * 24 * 3600_000,
};

const RANGE_LABEL: Record<Range, string> = {
    all: 'history_range_all',
    year: 'history_range_year',
    month: 'history_range_month',
};

function formatNumber(value: number | null): string {
    return value === null || value === undefined ? '—' : value.toLocaleString();
}

function formatBytes(bytes: number | null): string {
    if (bytes === null || bytes === undefined) {
        return '—';
    }
    if (bytes < 1024) {
        return `${bytes} B`;
    }
    const units = ['KB', 'MB', 'GB', 'TB'];
    let value = bytes / 1024;
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
        value /= 1024;
        unit++;
    }
    return `${value.toLocaleString(undefined, { maximumFractionDigits: value < 10 ? 2 : 1 })} ${units[unit]}`;
}

function formatTs(ts: number | null): string {
    return ts ? new Date(ts).toLocaleString() : '—';
}

const STATUS_COLOR: Record<DatapointStatus, 'success' | 'warning' | 'error'> = {
    active: 'success',
    loggingDisabled: 'warning',
    objectMissing: 'error',
};

const STATUS_LABEL: Record<DatapointStatus, string> = {
    active: 'history_status_active',
    loggingDisabled: 'history_status_disabled',
    objectMissing: 'history_status_missing',
};

export default class Statistics extends ConfigGeneric<ConfigGenericProps, StatisticsState> {
    constructor(props: ConfigGenericProps) {
        super(props);

        Object.assign(this.state, {
            stats: null,
            summary: null,
            loading: false,
            errorText: '',
            filter: '',
            range: 'all',
            sortField: 'count',
            sortAsc: false,
            preview: null,
            includeDisabled: false,
            deleting: false,
            doneText: '',
        });
    }

    async componentDidMount(): Promise<void> {
        await super.componentDidMount();
        if (this.props.alive) {
            await this.load();
        }
    }

    async componentDidUpdate(prevProps: ConfigGenericProps): Promise<void> {
        // the instance was started in the meantime
        if (this.props.alive && !prevProps.alive && !this.state.stats) {
            await this.load();
        }
    }

    get instance(): string {
        return `${this.props.oContext.adapterName}.${this.props.oContext.instance}`;
    }

    async sendToInstance(command: string, data: Record<string, any>): Promise<any> {
        const result = await this.props.oContext.socket.sendTo(this.instance, command, data);
        if (result?.error) {
            throw new Error(result.error);
        }
        return result;
    }

    async load(): Promise<void> {
        this.setState({ loading: true, errorText: '', doneText: '' });
        try {
            const span = RANGE_MS[this.state.range];
            const result = await this.sendToInstance(
                'getDpStatistics',
                span ? { start: Date.now() - span, end: Date.now() } : {},
            );
            this.setState({ stats: result?.result || [], summary: result?.summary || null, loading: false });
        } catch (e: any) {
            this.setState({ loading: false, stats: [], summary: null, errorText: e.message });
        }
    }

    /** Ask the adapter what a cleanup would remove, without removing anything yet */
    async requestPreview(includeDisabled: boolean): Promise<void> {
        this.setState({ loading: true, errorText: '' });
        try {
            const result = await this.sendToInstance('cleanupOrphaned', {
                scope: { objectMissing: true, loggingDisabled: includeDisabled },
            });
            this.setState({
                loading: false,
                includeDisabled,
                preview: {
                    datapoints: result.datapoints,
                    values: result.values,
                    bytes: result.bytes,
                    items: result.items || [],
                },
            });
        } catch (e: any) {
            this.setState({ loading: false, errorText: e.message });
        }
    }

    async confirmCleanup(): Promise<void> {
        this.setState({ deleting: true, errorText: '' });
        try {
            const result = await this.sendToInstance('cleanupOrphaned', {
                scope: { objectMissing: true, loggingDisabled: this.state.includeDisabled },
                confirm: true,
            });
            this.setState(
                {
                    deleting: false,
                    preview: null,
                    doneText: I18n.t(
                        'history_cleanup_done',
                        formatNumber(result.deleted.values),
                        formatNumber(result.deleted.datapoints),
                        formatBytes(result.deleted.bytes),
                    ),
                    // a file that could not be deleted must not be reported as removed
                    errorText: result.failed?.length ? I18n.t('history_cleanup_failed', result.failed.length) : '',
                },
                () => void this.load(),
            );
        } catch (e: any) {
            this.setState({ deleting: false, errorText: e.message });
        }
    }

    sortedStats(): DatapointStat[] {
        const { stats, filter, sortField, sortAsc } = this.state;
        const filterLower = filter.trim().toLowerCase();
        const list = (stats || []).filter(s => !filterLower || s.id.toLowerCase().includes(filterLower));

        const direction = sortAsc ? 1 : -1;
        return list.sort((a, b) => {
            const left = a[sortField];
            const right = b[sortField];
            if (left === right) {
                // a stable secondary key keeps the order from jumping around between renders
                return a.id > b.id ? 1 : a.id < b.id ? -1 : 0;
            }
            if (left === null || left === undefined) {
                return 1;
            }
            if (right === null || right === undefined) {
                return -1;
            }
            return (left > right ? 1 : -1) * direction;
        });
    }

    renderSortLabel(field: SortField, label: string): React.JSX.Element {
        return (
            <TableSortLabel
                active={this.state.sortField === field}
                direction={this.state.sortField === field && this.state.sortAsc ? 'asc' : 'desc'}
                onClick={() =>
                    this.setState({
                        sortField: field,
                        sortAsc: this.state.sortField === field ? !this.state.sortAsc : true,
                    })
                }
            >
                {I18n.t(label)}
            </TableSortLabel>
        );
    }

    renderSummary(): React.JSX.Element | null {
        const { summary } = this.state;
        if (!summary) {
            return null;
        }
        const missing = summary.byStatus.objectMissing;
        const disabled = summary.byStatus.loggingDisabled;

        return (
            <Box sx={{ display: 'flex', gap: 2, flexWrap: 'wrap', alignItems: 'center', mb: 1 }}>
                <Typography variant="body2">
                    {I18n.t(
                        'history_summary_total',
                        formatNumber(summary.datapoints),
                        formatNumber(summary.values),
                        formatBytes(summary.bytes),
                    )}
                </Typography>
                {!!missing.datapoints && (
                    <Chip
                        color="error"
                        size="small"
                        label={I18n.t(
                            'history_summary_missing',
                            formatNumber(missing.datapoints),
                            formatNumber(missing.values),
                            formatBytes(missing.bytes),
                        )}
                    />
                )}
                {!!disabled.datapoints && (
                    <Chip
                        color="warning"
                        size="small"
                        label={I18n.t(
                            'history_summary_disabled',
                            formatNumber(disabled.datapoints),
                            formatNumber(disabled.values),
                            formatBytes(disabled.bytes),
                        )}
                    />
                )}
            </Box>
        );
    }

    renderConfirmDialog(): React.JSX.Element | null {
        const { preview, includeDisabled, deleting } = this.state;
        if (!preview) {
            return null;
        }

        return (
            <Dialog
                open={!0}
                onClose={() => !deleting && this.setState({ preview: null })}
                maxWidth="md"
                fullWidth
            >
                <DialogTitle>{I18n.t('history_cleanup_confirm_title')}</DialogTitle>
                <DialogContent>
                    {preview.datapoints ? (
                        <>
                            <DialogContentText sx={{ mb: 1 }}>
                                {I18n.t(
                                    'history_cleanup_confirm_text',
                                    formatNumber(preview.datapoints),
                                    formatNumber(preview.values),
                                    formatBytes(preview.bytes),
                                )}
                            </DialogContentText>
                            <TableContainer
                                component={Paper}
                                sx={{ maxHeight: 320 }}
                            >
                                <Table
                                    size="small"
                                    stickyHeader
                                >
                                    <TableHead>
                                        <TableRow>
                                            <TableCell>{I18n.t('history_col_id')}</TableCell>
                                            <TableCell>{I18n.t('history_col_status')}</TableCell>
                                            <TableCell align="right">{I18n.t('history_col_count')}</TableCell>
                                            <TableCell align="right">{I18n.t('history_col_size')}</TableCell>
                                        </TableRow>
                                    </TableHead>
                                    <TableBody>
                                        {preview.items.map(item => (
                                            <TableRow key={item.id}>
                                                <TableCell sx={{ wordBreak: 'break-all' }}>{item.id}</TableCell>
                                                <TableCell>
                                                    <Chip
                                                        size="small"
                                                        color={STATUS_COLOR[item.status]}
                                                        label={I18n.t(STATUS_LABEL[item.status])}
                                                    />
                                                </TableCell>
                                                <TableCell align="right">{formatNumber(item.count)}</TableCell>
                                                <TableCell align="right">{formatBytes(item.bytes)}</TableCell>
                                            </TableRow>
                                        ))}
                                    </TableBody>
                                </Table>
                            </TableContainer>
                            {includeDisabled && (
                                <Alert
                                    severity="warning"
                                    icon={<Warning />}
                                    sx={{ mt: 1 }}
                                >
                                    {I18n.t('history_cleanup_disabled_warning')}
                                </Alert>
                            )}
                        </>
                    ) : (
                        <DialogContentText>{I18n.t('history_cleanup_nothing')}</DialogContentText>
                    )}
                </DialogContent>
                <DialogActions>
                    <Button
                        onClick={() => this.setState({ preview: null })}
                        disabled={deleting}
                        startIcon={<Close />}
                    >
                        {I18n.t('history_cancel')}
                    </Button>
                    <Button
                        variant="contained"
                        color="error"
                        disabled={deleting || !preview.datapoints}
                        startIcon={<DeleteSweep />}
                        onClick={() => void this.confirmCleanup()}
                    >
                        {I18n.t('history_cleanup_delete')}
                    </Button>
                </DialogActions>
            </Dialog>
        );
    }

    renderItem(): React.JSX.Element {
        if (!this.props.alive) {
            return <Alert severity="info">{I18n.t('history_stats_not_running')}</Alert>;
        }

        const rows = this.sortedStats();

        return (
            <Box sx={{ width: '100%' }}>
                <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', flexWrap: 'wrap', mb: 1 }}>
                    <TextField
                        variant="standard"
                        size="small"
                        value={this.state.filter}
                        placeholder={I18n.t('history_filter')}
                        onChange={e => this.setState({ filter: e.target.value })}
                        slotProps={{
                            input: {
                                endAdornment: this.state.filter ? (
                                    <InputAdornment position="end">
                                        <IconButton
                                            size="small"
                                            onClick={() => this.setState({ filter: '' })}
                                        >
                                            <Close />
                                        </IconButton>
                                    </InputAdornment>
                                ) : null,
                            },
                        }}
                    />
                    <Select
                        variant="standard"
                        size="small"
                        value={this.state.range}
                        onChange={e => this.setState({ range: e.target.value }, () => void this.load())}
                    >
                        {(Object.keys(RANGE_LABEL) as Range[]).map(range => (
                            <MenuItem
                                key={range}
                                value={range}
                            >
                                {I18n.t(RANGE_LABEL[range])}
                            </MenuItem>
                        ))}
                    </Select>
                    <Tooltip title={I18n.t('history_refresh')}>
                        <IconButton
                            onClick={() => void this.load()}
                            disabled={this.state.loading}
                        >
                            <Refresh />
                        </IconButton>
                    </Tooltip>
                    <Box sx={{ flexGrow: 1 }} />
                    <FormControlLabel
                        control={
                            <Checkbox
                                checked={this.state.includeDisabled}
                                onChange={e => this.setState({ includeDisabled: e.target.checked })}
                            />
                        }
                        label={I18n.t('history_include_disabled')}
                    />
                    <Button
                        variant="contained"
                        color="error"
                        startIcon={<DeleteSweep />}
                        disabled={this.state.loading}
                        onClick={() => void this.requestPreview(this.state.includeDisabled)}
                    >
                        {I18n.t('history_cleanup')}
                    </Button>
                </Box>

                {this.state.loading && <LinearProgress />}
                {!!this.state.errorText && (
                    <Alert
                        severity="error"
                        sx={{ mb: 1 }}
                        onClose={() => this.setState({ errorText: '' })}
                    >
                        {this.state.errorText}
                    </Alert>
                )}
                {!!this.state.doneText && (
                    <Alert
                        severity="success"
                        sx={{ mb: 1 }}
                        onClose={() => this.setState({ doneText: '' })}
                    >
                        {this.state.doneText}
                    </Alert>
                )}

                {this.renderSummary()}

                <TableContainer component={Paper}>
                    <Table
                        size="small"
                        stickyHeader
                    >
                        <TableHead>
                            <TableRow>
                                <TableCell>{this.renderSortLabel('id', 'history_col_id')}</TableCell>
                                <TableCell>{I18n.t('history_col_type')}</TableCell>
                                <TableCell>{this.renderSortLabel('status', 'history_col_status')}</TableCell>
                                <TableCell align="right">
                                    {this.renderSortLabel('count', 'history_col_count')}
                                </TableCell>
                                <TableCell align="right">{this.renderSortLabel('bytes', 'history_col_size')}</TableCell>
                                <TableCell align="right">
                                    {this.renderSortLabel('files', 'history_col_files')}
                                </TableCell>
                                <TableCell>{I18n.t('history_col_first')}</TableCell>
                                <TableCell>{this.renderSortLabel('lastTs', 'history_col_last')}</TableCell>
                            </TableRow>
                        </TableHead>
                        <TableBody>
                            {rows.map(row => (
                                <TableRow
                                    key={row.id}
                                    hover
                                >
                                    <TableCell sx={{ wordBreak: 'break-all' }}>{row.id}</TableCell>
                                    <TableCell>{row.type || '—'}</TableCell>
                                    <TableCell>
                                        <Chip
                                            size="small"
                                            color={STATUS_COLOR[row.status]}
                                            label={I18n.t(STATUS_LABEL[row.status])}
                                        />
                                    </TableCell>
                                    <TableCell align="right">{formatNumber(row.count)}</TableCell>
                                    <TableCell align="right">{formatBytes(row.bytes)}</TableCell>
                                    <TableCell align="right">{formatNumber(row.files)}</TableCell>
                                    <TableCell>{formatTs(row.firstTs)}</TableCell>
                                    <TableCell>{formatTs(row.lastTs)}</TableCell>
                                </TableRow>
                            ))}
                            {!rows.length && !this.state.loading && (
                                <TableRow>
                                    <TableCell colSpan={8}>
                                        <Typography
                                            variant="body2"
                                            sx={{ opacity: 0.7 }}
                                        >
                                            {I18n.t(
                                                this.state.filter ? 'history_stats_no_match' : 'history_no_datapoints',
                                            )}
                                        </Typography>
                                    </TableCell>
                                </TableRow>
                            )}
                        </TableBody>
                    </Table>
                </TableContainer>

                <Typography
                    variant="caption"
                    sx={{ display: 'block', mt: 1, opacity: 0.7 }}
                >
                    {I18n.t('history_stats_hint')}
                </Typography>

                {this.renderConfirmDialog()}
            </Box>
        );
    }
}
