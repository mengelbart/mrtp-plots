import datetime
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


def plot_rtp_rate(ax, df):
    df = df[df['msg'] == 'rtp packet'].copy()
    df['rate'] = df['rtp-packet.payload-length'] * 80
    df['timestamp'] = pd.to_datetime(df['time'])
    df.set_index('timestamp', inplace=True)
    df = df.resample('100ms').sum().copy()
    starttime = df.index[0]
    df['second'] = (df.index - starttime).total_seconds()
    df.set_index('second', inplace=True)

    ax.plot(df.index, df['rate'], linewidth=0.5)
    ax.set_ylim(bottom=0, top=6e6)
    ax.set_title('RTP Rate')
    ax.set_xlabel('Time')
    ax.set_ylabel('Rate')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='bit/s'))


def plot_rtp_owd(ax, rtp_tx_df, rtp_rx_df):
    rtp_tx_latency_df = rtp_tx_df.copy()
    rtp_rx_latency_df = rtp_rx_df.copy()
    rtp_tx_latency_df['ts'] = rtp_tx_df.index
    rtp_rx_latency_df['ts'] = rtp_rx_df.index
    df = rtp_tx_latency_df.merge(rtp_rx_latency_df, on='extseq')
    df['latency'] = (df['ts_y'] - df['ts_x']) / \
        datetime.timedelta(milliseconds=1) / 1000.0

    df['ts'] = pd.to_datetime(df['ts_x'])
    df.set_index('ts', inplace=True)
    starttime = df.index[0]
    df['second'] = (df.index - starttime).total_seconds()
    df.set_index('second', inplace=True)

    ax.plot(df.index, df['latency'], label='Latency', linewidth=0.5)
    ax.set_title('Latency')
    ax.set_ylim(bottom=0, top=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Latency')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
