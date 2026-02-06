import polars as pl
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path

FILE_FORMAT = 'png'
DEFAULT_LINE_WIDTH = 1.0


def gr(x):
    return x * (5**.5 - 1) / 2


class Plotter:
    def __init__(self, input, output, output_format='png'):
        self.input = input
        self.output = output
        self.output_format = output_format

    def plot(self):
        dfs = {
            p.stem: pl.read_parquet(p)
            for p in Path(self.input).glob('*.parquet')
        }
        # add time_delta column to all dfs with a 'time' column
        start_time = dfs['config']['time'][0]
        for k in dfs:
            if 'time' in dfs[k].columns:
                dfs[k] = dfs[k].with_columns(
                    (pl.col('time') - start_time).alias('time_delta')
                    .cast(pl.Duration('us'))
                )

        Path(self.output).mkdir(parents=True, exist_ok=True)
        self.plot_loss_rate(dfs)
        self.plot_rate(dfs)
        self.plot_delay(dfs)
        self.plot_video_quality(dfs)

    def plot_rate(self, dfs):
        width = 8
        height = gr(width)
        fig, ax = plt.subplots(figsize=(width, height), layout='constrained')
        plot_capacity(ax, dfs['tc'])
        if 'metrics' in dfs:
            plot_target_rate(ax, dfs['metrics'])
        if 'rtp_packets' in dfs:
            plot_rtp_packets_rate(ax, dfs['rtp_packets'])
        if 'qlog-sender-packets' in dfs and 'qlog-receiver-packets' in dfs:
            plot_qlog_packets_rate(ax, dfs['qlog-sender-packets'],
                                   dfs['qlog-receiver-packets'])
        ax.set_xlabel('Time')
        ax.set_ylabel('Rate (MBit/s)')
        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, pos: f'{x/1e6:.0f}s'))
        ax.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, pos: f'{x/1e6}'))
        ax.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
                  ncols=2, mode="expand", borderaxespad=0.)
        fig.savefig(Path(self.output) / f'rate.{FILE_FORMAT}')
        plt.close(fig)

    def plot_loss_rate(self, dfs):
        if 'rtp_packets' not in dfs:
            return
        width = 8
        height = gr(width)
        fig, ax = plt.subplots(figsize=(width, height), layout='constrained')
        plot_loss_between(ax, dfs['rtp_packets'], 'time', 'time_rx')
        ax.set_xlabel('Time')
        ax.set_ylabel('Loss Rate (%)')
        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
        ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
        ax.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
                  ncols=2, mode="expand", borderaxespad=0.)
        fig.savefig(Path(self.output) / f'loss.{FILE_FORMAT}')
        plt.close(fig)

    def plot_delay(self, dfs):
        if 'rtp_packets' not in dfs:
            return
        width = 8
        height = gr(width)
        fig, ax = plt.subplots(figsize=(width, height), layout='constrained')
        df = dfs['rtp_packets']
        plot_delay_from_to(ax, df, 'time', 'time_rx',
                           linewidth=DEFAULT_LINE_WIDTH, linestyle='--',
                           label='tx->rx')

        if 'time_ns4' in df.columns and 'time_ns1' in df.columns:
            plot_delay_from_to(ax, df, 'time_ns4', 'time_ns1',
                               linewidth=DEFAULT_LINE_WIDTH, linestyle='--',
                               label='ns4->ns1')

        if 'time_ns3' in df.columns and 'time_ns2' in df.columns:
            plot_delay_from_to(ax, df, 'time_ns3', 'time_ns2',
                               linewidth=DEFAULT_LINE_WIDTH, linestyle='--',
                               label='ns3->ns2')

        if 'time_ns4' in df.columns:
            plot_delay_from_to(ax, df, 'time', 'time_ns4',
                               linewidth=DEFAULT_LINE_WIDTH, linestyle='--',
                               label='tx->ns4')

        if 'time_ns1' in df.columns:
            plot_delay_from_to(ax, df, 'time_ns1', 'time_rx',
                               linewidth=DEFAULT_LINE_WIDTH, linestyle='--',
                               label='ns1->rx')

        ax.legend(bbox_to_anchor=(0., 1.02, 1., .102), loc='lower left',
                  ncols=2, mode="expand", borderaxespad=0.)
        ax.set_xlabel('Time')
        ax.set_ylabel('Latency')
        ax.xaxis.set_major_formatter(
            mticker.FuncFormatter(lambda x, pos: f'{x/1e6:.0f}s'))
        ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
        fig.savefig(Path(self.output) / f'latency.{FILE_FORMAT}')
        plt.close(fig)

    def plot_video_quality(self, dfs):
        if 'video_quality' not in dfs:
            return
        width = 16
        height = gr(width)
        fig, ax = plt.subplots(nrows=2, figsize=(width, height),
                               layout='constrained')

        ax_psnr = ax[0]
        ax_ssim = ax_psnr.twinx()

        psnr = (
            dfs['video_quality']
            .filter(pl.col('variable') == 'psnr_avg')
        )
        ssim = (
            dfs['video_quality']
            .filter(pl.col('variable') == 'ssim_avg')
        )
        ax_psnr.plot(psnr['n'], psnr['value'], linestyle='-', marker='',
                     label='PSNR avg', color='tab:blue')
        ax_ssim.plot(ssim['n'], ssim['value'], linestyle='-', marker='',
                     label='SSIM avg', color='tab:orange')

        ax_psnr.set_xlabel('Frames')
        ax_psnr.set_ylabel('PSNR (dB)', color='tab:blue')
        ax_ssim.set_ylabel('SSIM', color='tab:orange')

        ax_psnr.tick_params(axis='y', labelcolor='tab:blue')
        ax_ssim.tick_params(axis='y', labelcolor='tab:orange')

        ax_psnr.grid(True, axis='y', linestyle='--', alpha=0.3,
                     color='tab:blue')

        ax_psnr.legend(loc='upper left')
        ax_ssim.legend(loc='upper right')

        ax_psnr_cdf = ax[1]
        ax_ssim_cdf = ax_psnr_cdf.twiny()

        ax_psnr_cdf.ecdf(psnr['value'], label='PSNR avg', color='tab:blue')
        ax_ssim_cdf.ecdf(ssim['value'], label='SSIM avg', color='tab:orange')

        ax_psnr_cdf.tick_params(axis='x', labelcolor='tab:blue')
        ax_ssim_cdf.tick_params(axis='x', labelcolor='tab:orange')

        ax_psnr_cdf.set_xlabel('PSNR', color='tab:blue')
        ax_ssim_cdf.set_xlabel('SSIM', color='tab:orange')
        ax_psnr_cdf.set_ylabel('CDF')

        ax_psnr_cdf.legend(loc='upper left')
        ax_ssim_cdf.legend(loc='upper right')

        fig.savefig(Path(self.output) / f'video_quality.{FILE_FORMAT}')
        plt.close(fig)


def plot_capacity(ax, tc_df):
    capacity = (
        tc_df
        .filter(pl.col('variable') == 'bandwidth')
    )
    ax.step(capacity['time_delta'], capacity['value'], where='post',
            label='Bandwidth', linewidth=DEFAULT_LINE_WIDTH, color='grey')


def plot_target_rate(ax, metrics_df):
    target = metrics_df.filter(pl.col('metric') == 'target-rate')
    ax.plot(target['time_delta'], target['value'], label='Target Rate',
            linewidth=DEFAULT_LINE_WIDTH)


def plot_rtp_packets_rate(ax, packets_df):
    tx_rate = (
        packets_df.filter(pl.col('time').is_not_null())
        .group_by_dynamic('time', every='1s')
        .agg(
            pl.col('payload-length').sum() * 8,
            pl.col('time_delta').min()
        )
    )
    ax.plot(tx_rate['time_delta'], tx_rate['payload-length'],
            label='Transmission Rate', linewidth=DEFAULT_LINE_WIDTH)

    rx_rate = (
        packets_df.filter(pl.col('time_rx').is_not_null())
        .group_by_dynamic('time', every='1s')
        .agg(
            pl.col('payload-length').sum() * 8,
            pl.col('time_delta').min()
        )
    )
    ax.plot(rx_rate['time_delta'], rx_rate['payload-length'],
            label='Delivery Rate', linewidth=DEFAULT_LINE_WIDTH)


def plot_qlog_packets_rate(ax, tx_df, rx_df):
    tx_rate = (
        tx_df
        .filter(pl.col('name') == 'transport:packet_sent')
        .group_by_dynamic('time', every='1s')
        .agg(
            pl.col('data.raw.length').sum() * 8,
            pl.col('time_delta').min()
        )
    )
    ax.plot(tx_rate['time_delta'], tx_rate['data.raw.length'],
            label='QUIC Tx Rate', linewidth=DEFAULT_LINE_WIDTH)
    rx_rate = (
        rx_df
        .filter(pl.col('name') == 'transport:packet_received')
        .group_by_dynamic('time', every='1s')
        .agg(
            pl.col('data.raw.length').sum() * 8,
            pl.col('time_delta').min()
        )
    )
    ax.plot(rx_rate['time_delta'], rx_rate['data.raw.length'],
            label='QUIC Rx Rate', linewidth=DEFAULT_LINE_WIDTH)


def plot_delay_from_to(ax, packets_df, a, b, **kwargs):
    delay = (
        packets_df
        .select(pl.col(['time_delta', a, b]))
        .filter(
            pl.col(a).is_not_null(),
            pl.col(b).is_not_null(),
        ).with_columns(
            (pl.col(b) - pl.col(a))
            .dt.total_seconds(fractional=True).alias('latency')
        )
    )
    ax.plot(delay['time_delta'], delay['latency'], **kwargs)


def plot_loss_between(ax, packets_df, src, dst, total=False):
    loss = (
        packets_df
        .with_columns(pl.col('time_delta').dt.total_seconds())
        .sort('time_delta')
        .group_by('time_delta')
        .agg([
            pl.col(src).count().alias('sent'),
            pl.col(dst).null_count().alias('lost'),
        ])
        .with_columns(
            (pl.col('lost') / pl.col('sent')).alias('rate')
        )
    )
    if total:
        ax.bar(loss['time_delta'], loss['lost'])
    else:
        ax.plot(loss['time_delta'], loss['rate'], label='Lossrate',
                linewidth=DEFAULT_LINE_WIDTH)
