import datetime
import matplotlib.ticker as mticker
import pandas as pd
import re

unit_multipliers = {
    'bit': 1,
    'kbit': 1_000,
    'mbit': 1_000_000,
    'gbit': 1_000_000_000,
}

usage_and_state = {
    -1: 'over / decrease',
    0: 'hold / normal',
    1: 'under / increase',
}


def parse_rate(rate_str):
    match = re.match(r'([\d.]+)([a-zA-Z]+)', rate_str.strip())
    if not match:
        raise ValueError(f'invalid rate format {rate_str}')
    value, unit = match.groups()
    unit = unit.lower()
    if unit not in unit_multipliers:
        raise ValueError(f'unknown unit {unit}')
    return float(value) * unit_multipliers[unit]


def set_start_time_index(df, start_time, time_column):
    df['timestamp'] = pd.to_datetime(df[time_column])
    df.set_index('timestamp', inplace=True)
    df['second'] = (df.index - start_time).total_seconds()
    df.set_index('second', inplace=True)
    return df


def plot_rtp_rates(ax, start_time, cap_df, tx_df, rx_df):
    plot_capacity(ax, start_time, cap_df)
    plot_rtp_rate(ax, start_time, tx_df, 'tx')
    plot_rtp_rate(ax, start_time, rx_df, 'rx')
    plot_target_rate(ax, start_time, tx_df)
    # ax.set_ylim(bottom=0, top=6e6)
    ax.set_xlabel('Time')
    ax.set_ylabel('Rate')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='bit/s'))
    ax.legend(loc='upper right')
    return True


def plot_capacity(ax, start_time, df):
    df['rate'] = df['bandwidth'].apply(parse_rate)
    df = set_start_time_index(df, start_time, 'time')
    ax.step(df.index, df['rate'], where='post',
            label='capacity', linewidth=0.5)
    return True


def plot_target_rate(ax, start_time, df):
    df = df[df['msg'] == 'NEW_TARGET_RATE'].copy()
    if df.empty:
        return
    df = set_start_time_index(df, start_time, 'time')
    ax.plot(df.index, df['rate'], label='target', linewidth=0.5)
    return True


def plot_rtp_rate(ax, start_time, df, label):
    df = df[df['msg'] == 'rtp packet'].copy()
    df['rate'] = df['rtp-packet.payload-length'] * 80
    df['timestamp'] = pd.to_datetime(df['time'])
    df.set_index('timestamp', inplace=True)
    df = df.resample('100ms').sum().copy()
    df['second'] = (df.index - start_time).total_seconds()
    df.set_index('second', inplace=True)
    ax.plot(df.index, df['rate'], label=label, linewidth=0.5)
    return True


def plot_rtp_loss_pcap(ax, start_time, rtp_tx_df, rtp_rx_df):
    return _plot_rtp_loss(ax, start_time, rtp_tx_df, rtp_rx_df, 'extseq')


def plot_rtp_loss_log(ax, start_time, rtp_tx_df, rtp_rx_df):
    rtp_tx_df = rtp_tx_df[rtp_tx_df['msg'] == 'rtp packet'].copy()
    rtp_rx_df = rtp_rx_df[rtp_rx_df['msg'] == 'rtp packet'].copy()

    return _plot_rtp_loss(ax, start_time, rtp_tx_df, rtp_rx_df, 'rtp-packet.sequence-number')


def _plot_rtp_loss(ax, start_time, rtp_tx_df, rtp_rx_df, seq_nr_name):
    rtp_tx_df = rtp_tx_df.reset_index()
    rtp_rx_df = rtp_rx_df.reset_index()
    tx_df = rtp_tx_df[['time', seq_nr_name]]
    rx_df = rtp_rx_df[['time', seq_nr_name]]
    merged_df = pd.merge(tx_df, rx_df, on=seq_nr_name,
                         how='left', indicator=True)
    merged_df['tx'] = pd.to_datetime(merged_df['time_x'])
    merged_df['second'] = merged_df['tx'].dt.floor('s')
    merged_df['lost'] = merged_df['_merge'] == 'left_only'
    merged_df = merged_df.groupby('second').agg(
        sent=(seq_nr_name, 'count'),
        lost=('lost', 'sum')
    )
    merged_df['loss_rate'] = merged_df['lost'] / merged_df['sent']

    merged_df['second'] = (merged_df.index - start_time).total_seconds()
    merged_df.set_index('second', inplace=True)

    ax.plot(merged_df.index, merged_df['loss_rate'], linewidth=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Loss Rate')
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    return True


def plot_rtp_owd_pcap(ax, start_time, rtp_tx_df, rtp_rx_df):
    rtp_tx_latency_df = rtp_tx_df.copy()
    rtp_rx_latency_df = rtp_rx_df.copy()
    rtp_tx_latency_df['ts'] = rtp_tx_df.index
    rtp_rx_latency_df['ts'] = rtp_rx_df.index
    return _plot_rtp_owd(ax, start_time, rtp_tx_latency_df, rtp_rx_latency_df, 'extseq')


def plot_rtp_owd_log(ax, start_time, rtp_tx_df, rtp_rx_df):
    rtp_tx_latency_df = rtp_tx_df[rtp_tx_df['msg'] == 'rtp packet'].copy()
    rtp_rx_latency_df = rtp_rx_df[rtp_rx_df['msg'] == 'rtp packet'].copy()
    rtp_tx_latency_df['ts'] = pd.to_datetime(rtp_tx_latency_df['time'])
    rtp_rx_latency_df['ts'] = pd.to_datetime(rtp_rx_latency_df['time'])
    return _plot_rtp_owd(ax, start_time, rtp_tx_latency_df, rtp_rx_latency_df, 'rtp-packet.unwrapped-sequence-number')


def _plot_rtp_owd(ax, start_time, rtp_tx_latency_df, rtp_rx_latency_df, seq_nr_name):
    merged_df = rtp_tx_latency_df.merge(rtp_rx_latency_df, on=seq_nr_name)
    merged_df['latency'] = (merged_df['ts_y'] - merged_df['ts_x']) / \
        datetime.timedelta(milliseconds=1) / 1000.0
    df = set_start_time_index(merged_df, start_time, 'ts_x')
    ax.plot(df.index, df['latency'], label='Latency', linewidth=0.5)
    # ax.set_ylim(bottom=0, top=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Latency')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
    return True


def plot_scream_queue_delay(ax, start_time, df):
    df = df[df['msg'] == 'SCReAM stats'].copy()
    if df.empty:
        return False
    df = set_start_time_index(df, start_time, 'time')
    ax.plot(df.index, df['queueDelay'], label='queueDelay', linewidth=0.5)
    ax.plot(df.index, df['queueDelayMax'],
            label='queueDelayMax', linewidth=0.5)
    ax.plot(df.index, df['queueDelayMinAvg'],
            label='queueDelayMinAvg', linewidth=0.5)
    ax.plot(df.index, df['sRtt'], label='sRtt', linewidth=0.5)
    ax.plot(df.index, df['rtpQueueDelay'],
            label='rtpQueueDelay', linewidth=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Delay')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
    ax.legend(loc='upper right')
    return True


def plot_scream_cwnd(ax, start_time, df):
    df = df[df['msg'] == 'SCReAM stats'].copy()
    if df.empty:
        return False
    df = set_start_time_index(df, start_time, 'time')
    ax.plot(df.index, df['cwnd'], label='cwnd', linewidth=0.5)
    ax.plot(df.index, df['bytesInFlightLog'],
            label='bytesInFlightLog', linewidth=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Size')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='B'))
    ax.legend(loc='upper right')
    return True


def plot_gcc_rtt(ax, start_time, df):
    df = df[df['msg'] == 'pion-trace-log'].copy()
    if df.empty:
        return False
    if not 'rtt' in df.columns:
        return False
    df = set_start_time_index(df, start_time, 'time')
    df['rtt'] = df['rtt']*1e-9
    df = df.dropna(subset=['rtt'])
    ax.plot(df.index, df['rtt'], label='RTT', linewidth=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Size')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
    ax.legend(loc='upper right')
    return True


def plot_gcc_target_rates(ax, start_time, df):
    df = df[df['msg'] == 'pion-trace-log'].copy()
    if df.empty:
        return False
    if 'loss-target' not in df.columns or 'delay-target' not in df.columns:
        return False
    df = df.dropna(subset=['loss-target', 'delay-target', 'target'])
    df = set_start_time_index(df, start_time, 'time')
    ax.plot(df.index, df['loss-target'], label='loss-target', linewidth=0.5)
    ax.plot(df.index, df['delay-target'], label='delay-target', linewidth=0.5)
    ax.plot(df.index, df['target'], label='target', linewidth=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Rate')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='b/s'))
    ax.legend(loc='upper right')
    return True


def plot_gcc_estimates(ax, start_time, df):
    df = df[df['msg'] == 'pion-trace-log'].copy()
    if df.empty:
        return False
    if 'estimate' not in df.columns:
        return False
    df = set_start_time_index(df, start_time, 'time')
    df['interGroupDelay'] = df['interGroupDelay'] * 1e-3
    df['estimate'] = df['estimate'] * 60
    df = df.dropna(subset=['estimate', 'threshold'])
    ax.plot(df.index, df['interGroupDelay'],
            label='interGroupDelay', linewidth=0.5)
    ax.plot(df.index, df['estimate'], label='estimate', linewidth=0.5)
    ax.plot(df.index, df['threshold'], label='threshold', linewidth=0.5)
    ax.plot(df.index, -df['threshold'], label='-threshold', linewidth=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Time')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
    ax.legend(loc='upper right')
    return True


def plot_gcc_usage_and_state(ax, start_time, df):
    df = df[df['msg'] == 'pion-trace-log'].copy()
    if df.empty:
        return False
    if 'usage' not in df.columns or 'state' not in df.columns:
        return False
    df = set_start_time_index(df, start_time, 'time')
    df = df.dropna(subset=['usage', 'state'])
    df['usage'] = -df['usage']
    ax.step(df.index, df['usage'], where='post', label='usage', linewidth=0.5)
    ax.step(df.index, df['state'], where='post', label='state', linewidth=0.5)
    ax.set_xlabel('Time')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: usage_and_state.get(x, '')))
    ax.legend(loc='upper right')
    return True


def plot_frame_sizes(ax, start_time, df):
    encoding = df[df['msg'] == 'encoding frame']
    encoded = df[df['msg'] == 'encoded frame']
    decoding = df[df['msg'] == 'decoding frame']
    decoded = df[df['msg'] == 'decoded frame']
    plotted = False
    if (not encoding.empty) or (not encoded.empty):
        plotted = _plot_frame_sizes(
            ax, encoding, 'encoding', encoded, 'encoded')
    if not decoding.empty or not decoded.empty:
        plotted = _plot_frame_sizes(
            ax, decoding, 'decoding', decoded, 'decoded')
    return plotted


def _plot_frame_sizes(ax, df_a, label_a, df_b, label_b):
    df_a = df_a.reset_index()
    df_b = df_b.reset_index()
    ax.bar(df_a.index, df_a['size'], width=0.25, label=label_a)
    ax.bar(df_b.index+0.25, df_b['size'], width=0.25, label=label_b)
    ax.legend(loc='upper right')
    return True


def plot_coding_time(ax, start_time, df):
    pass
