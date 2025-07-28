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


def plot_rtp_rates(ax, start_time, cap_df, tx_df, rx_df):
    plot_capacity(ax, start_time, cap_df)
    plot_rtp_rate(ax, start_time, tx_df, 'tx')
    plot_rtp_rate(ax, start_time, rx_df, 'rx')
    plot_target_rate(ax, start_time, tx_df)
    ax.set_ylim(bottom=0, top=6e6)
    ax.set_xlabel('Time')
    ax.set_ylabel('Rate')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='bit/s'))
    ax.legend(loc='upper right')


def parse_rate(rate_str):
    match = re.match(r'([\d.]+)([a-zA-Z]+)', rate_str.strip())
    if not match:
        raise ValueError(f'invalid rate format {rate_str}')
    value, unit = match.groups()
    unit = unit.lower()
    if unit not in unit_multipliers:
        raise ValueError(f'unknown unit {unit}')
    return float(value) * unit_multipliers[unit]


def plot_capacity(ax, start_time, df):
    df['rate'] = df['bandwidth'].apply(parse_rate)
    df['timestamp'] = pd.to_datetime(df['time'])
    df.set_index('timestamp', inplace=True)
    df['second'] = (df.index - start_time).total_seconds()
    df.set_index('second', inplace=True)
    ax.step(df.index, df['rate'], where='post',
            label='capacity', linewidth=0.5)


def plot_target_rate(ax, start_time, df):
    df = df[df['msg'] == 'NEW_TARGET_RATE'].copy()
    if df.empty:
        return
    df['timestamp'] = pd.to_datetime(df['time'])
    df.set_index('timestamp', inplace=True)
    df['second'] = (df.index - start_time).total_seconds()
    df.set_index('second', inplace=True)
    ax.plot(df.index, df['rate'], label='target', linewidth=0.5)


def plot_rtp_rate(ax, start_time, df, label):
    df = df[df['msg'] == 'rtp packet'].copy()
    df['rate'] = df['rtp-packet.payload-length'] * 80
    df['timestamp'] = pd.to_datetime(df['time'])
    df.set_index('timestamp', inplace=True)
    df = df.resample('100ms').sum().copy()
    df['second'] = (df.index - start_time).total_seconds()
    df.set_index('second', inplace=True)
    ax.plot(df.index, df['rate'], label=label, linewidth=0.5)


def plot_rtp_loss(ax, start_time, rtp_tx_df, rtp_rx_df):
    rtp_tx_df = rtp_tx_df.reset_index()
    rtp_rx_df = rtp_rx_df.reset_index()
    tx_df = rtp_tx_df[['time', 'extseq']]
    rx_df = rtp_rx_df[['time', 'extseq']]
    df = pd.merge(tx_df, rx_df, on='extseq', how='left', indicator=True)
    df['tx'] = pd.to_datetime(df['time_x'])
    df['second'] = df['tx'].dt.floor('s')
    df['lost'] = df['_merge'] == 'left_only'
    df = df.groupby('second').agg(
        sent=('extseq', 'count'),
        lost=('lost', 'sum')
    )
    df['loss_rate'] = df['lost'] / df['sent']

    df['second'] = (df.index - start_time).total_seconds()
    df.set_index('second', inplace=True)

    ax.plot(df.index, df['loss_rate'], linewidth=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Loss Rate')
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))


def plot_rtp_owd(ax, start_time, rtp_tx_df, rtp_rx_df):
    rtp_tx_latency_df = rtp_tx_df.copy()
    rtp_rx_latency_df = rtp_rx_df.copy()
    rtp_tx_latency_df['ts'] = rtp_tx_df.index
    rtp_rx_latency_df['ts'] = rtp_rx_df.index
    df = rtp_tx_latency_df.merge(rtp_rx_latency_df, on='extseq')
    df['latency'] = (df['ts_y'] - df['ts_x']) / \
        datetime.timedelta(milliseconds=1) / 1000.0

    df['ts'] = pd.to_datetime(df['ts_x'])
    df.set_index('ts', inplace=True)
    df['second'] = (df.index - start_time).total_seconds()
    df.set_index('second', inplace=True)

    ax.plot(df.index, df['latency'], label='Latency', linewidth=0.5)
    ax.set_ylim(bottom=0, top=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Latency')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
