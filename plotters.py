import datetime
import re
import matplotlib.ticker as mticker
import pandas as pd

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


def _name_space_to_ip(namespace):
    # TODO: only works for these two namespaces
    match namespace:
        case "ns1":
            return '10.1.0.10'
        case "ns4":
            return '10.3.0.20'
    raise ValueError(
        f'dlts plotting only works for the namespaces ns1 and ns4, and not for {namespace}')


def _get_ips_from_config(config_df):
    sender_ip = '0.0.0.0'
    receiver_ip = '0.0.0.0'

    for _, row in config_df['applications'].items():
        for app in row:
            if app['name'] == "sender":
                sender_ip = _name_space_to_ip(app['namespace'])
            if app['name'] == "receiver":
                receiver_ip = _name_space_to_ip(app['namespace'])

    return sender_ip, receiver_ip


def set_start_time_index(df, start_time, time_column):
    df['timestamp'] = pd.to_datetime(df[time_column])
    df.set_index('timestamp', inplace=True)
    df['second'] = (df.index - start_time).total_seconds()
    df.set_index('second', inplace=True)
    return df


def plot_rtp_rates_log(ax, start_time, cap_df, tx_df, rx_df):
    """ plots rtp rates from logs"""
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(ax, start_time, tx_df)
    plot_rtp_rate(ax, start_time, tx_df, 'tx')
    plot_rtp_rate(ax, start_time, rx_df, 'rx')
    _rate_plot_ax_config(ax)
    return True


def plot_rtp_rates_pcaps(ax, start_time, cap_df, tx_log_df, rtp_tx_df, rtp_rx_df, config_df):
    """plots rtp rates from pcaps"""
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(ax, start_time, tx_log_df)

    sender_ip, receiver_ip = _get_ips_from_config(config_df)

    rtp_tx_df = rtp_tx_df[rtp_tx_df['src'] == sender_ip].copy()
    rtp_tx_df['rate'] = rtp_tx_df['length'] * 80
    _plot_rate(ax, start_time, rtp_tx_df, 'tx')

    rtp_rx_df = rtp_rx_df[rtp_rx_df['dst'] == receiver_ip].copy()
    rtp_rx_df['rate'] = rtp_rx_df['length'] * 80
    _plot_rate(ax, start_time, rtp_rx_df, 'rx')

    _rate_plot_ax_config(ax)
    return True


def plot_quic_rates(ax, start_time, cap_df, tx_log_df, qlog_tx_df, qlog_rx_df):
    """plots quic rates from qlogs"""

    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(ax, start_time, tx_log_df)

    quic_tx_latency_df = qlog_tx_df[qlog_tx_df['name']
                                    == 'transport:packet_sent'].copy()
    quic_rx_latency_df = qlog_rx_df[qlog_rx_df['name']
                                    == 'transport:packet_received'].copy()

    if quic_tx_latency_df.empty or quic_rx_latency_df.empty:
        return False

    quic_tx_latency_df['rate'] = quic_tx_latency_df['data.raw.length'] * 80
    _plot_data_rate(ax, start_time, quic_tx_latency_df, 'tx')

    quic_rx_latency_df['rate'] = quic_rx_latency_df['data.raw.length'] * 80
    _plot_data_rate(ax, start_time, quic_rx_latency_df, 'rx')

    _rate_plot_ax_config(ax)
    return True


def plot_all_send_rates(ax, start_time, cap_df, tx_df):
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(
        ax, start_time, tx_df, event_name='NEW_TARGET_RATE')
    tx_data_plotted, data_df = plot_data_rate(ax, start_time, tx_df, 'data')

    # only plot if data was sent
    if not tx_data_plotted:
        return False

    _, media_df = plot_rtp_rate(ax, start_time, tx_df, 'media')

    # sum graph
    combined_df = data_df.join(
        media_df, how='outer', lsuffix='_data', rsuffix='_media')
    combined_df['rate'] = combined_df.get(
        'rate_data', 0) + combined_df.get('rate_media', 0)
    ax.plot(combined_df.index,
            combined_df['rate'], label='total', linewidth=0.5)

    _rate_plot_ax_config(ax)
    return True


def plot_all_recv_rates(ax, start_time, cap_df, tx_df, rx_df):
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(
        ax, start_time, tx_df, event_name='NEW_TARGET_RATE')
    rx_plotted, data_df = plot_data_rate(
        ax, start_time, rx_df, 'data', event_name='DataSink received data')

    # only plot if data was sent
    if not rx_plotted:
        return False

    _, media_df = plot_rtp_rate(ax, start_time, rx_df, 'media')

    # sum graph
    combined_df = data_df.join(
        media_df, how='outer', lsuffix='_data', rsuffix='_media')
    combined_df['rate'] = combined_df.get(
        'rate_data', 0) + combined_df.get('rate_media', 0)
    ax.plot(combined_df.index,
            combined_df['rate'], label='total', linewidth=0.5)

    _rate_plot_ax_config(ax)
    return True


def _rate_plot_ax_config(ax):
    # ax.set_ylim(bottom=0, top=6e6)
    ax.set_xlabel('Time')
    ax.set_ylabel('Rate')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='bit/s'))
    ax.legend(loc='upper right')


def plot_capacity(ax, start_time, df):
    if not df.empty:
        df['rate'] = df['bandwidth'].apply(parse_rate)
        df = set_start_time_index(df, start_time, 'time')
        ax.step(df.index, df['rate'], where='post',
                label='capacity', linewidth=0.5, color="lightskyblue")


def plot_target_rate(ax, start_time, df, event_name='NEW_TARGET_MEDIA_RATE'):
    df = df[df['msg'] == event_name].copy()
    if df.empty:
        return False
    df = set_start_time_index(df, start_time, 'time')
    ax.plot(df.index, df['rate'], label='target', linewidth=0.5)
    return True


def plot_rtp_rate(ax, start_time, df, label):
    df = df[df['msg'] == 'rtp packet'].copy()
    if df.empty:
        return False, df

    df['rate'] = df['rtp-packet.payload-length'] * 80

    return _plot_data_rate(ax, start_time, df, label)


def plot_data_rate(ax, start_time, df, label, event_name='DataSource sent data'):
    df = df[df['msg'] == event_name].copy()
    if df.empty:
        return False, df

    df['rate'] = df['payload-length'] * 80

    return _plot_data_rate(ax, start_time, df, label)


def _plot_rate(ax, start_time, df, label):
    """time as index and rate as column"""
    df = df.resample('100ms').sum(numeric_only=True).copy()

    df['second'] = (df.index - start_time).total_seconds()
    df.set_index('second', inplace=True)
    ax.plot(df.index, df['rate'], label=label, linewidth=0.5)

    return True, df


def _plot_data_rate(ax, start_time, df, label):
    df.set_index('time', inplace=True)
    return _plot_rate(ax, start_time, df, label)


def plot_rtp_loss_pcap(ax, start_time, rtp_tx_df, rtp_rx_df):
    return _plot_rtp_loss(ax, start_time, rtp_tx_df, rtp_rx_df, 'extseq')


def plot_rtp_loss_log(ax, start_time, rtp_tx_df, rtp_rx_df):
    rtp_tx_df = rtp_tx_df[rtp_tx_df['msg'] == 'rtp packet'].copy()
    rtp_rx_df = rtp_rx_df[rtp_rx_df['msg'] == 'rtp packet'].copy()

    return _plot_rtp_loss(ax, start_time, rtp_tx_df, rtp_rx_df, 'rtp-packet.sequence-number')


def _plot_rtp_loss(ax, start_time, rtp_tx_df, rtp_rx_df, seq_nr_name):
    if rtp_tx_df.empty:
        return False
    if rtp_rx_df.empty:
        rtp_rx_df = pd.DataFrame(columns=rtp_tx_df.columns)

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
    return _plot_owd(ax, start_time, rtp_tx_latency_df, rtp_rx_latency_df, 'extseq')


def plot_dlts_owd(ax, start_time, dlts_tx_df, dlts_rx_df, config_df):
    sender_ip, receiver_ip = _get_ips_from_config(config_df)

    dlts_tx_latency_df = dlts_tx_df[dlts_tx_df['src'] == sender_ip].copy()
    dlts_rx_latency_df = dlts_rx_df[dlts_rx_df['dst'] == receiver_ip].copy()
    dlts_tx_latency_df['ts'] = dlts_tx_latency_df.index
    dlts_rx_latency_df['ts'] = dlts_rx_latency_df.index

    return _plot_owd(ax, start_time, dlts_tx_latency_df, dlts_rx_latency_df, 'seq')


def plot_dlts_loss(ax, start_time, dlts_tx_df, dlts_rx_df, config_df):
    sender_ip, receiver_ip = _get_ips_from_config(config_df)

    dlts_tx_latency_df = dlts_tx_df[dlts_tx_df['src'] == sender_ip].copy()
    dlts_rx_latency_df = dlts_rx_df[dlts_rx_df['dst'] == receiver_ip].copy()

    return _plot_rtp_loss(ax, start_time, dlts_tx_latency_df, dlts_rx_latency_df, 'seq')


def plot_dlts_rates(ax, start_time, cap_df, tx_df, dlts_tx_df, dlts_rx_df, config_df):
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(ax, start_time, tx_df)

    sender_ip, receiver_ip = _get_ips_from_config(config_df)

    dlts_tx_df = dlts_tx_df[dlts_tx_df['src'] == sender_ip].copy()
    dlts_tx_df['rate'] = dlts_tx_df['length'] * 80
    _plot_rate(ax, start_time, dlts_tx_df, 'tx')

    dlts_rx_df = dlts_rx_df[dlts_rx_df['dst'] == receiver_ip].copy()
    dlts_rx_df['rate'] = dlts_rx_df['length'] * 80
    _plot_rate(ax, start_time, dlts_rx_df, 'rx')

    _rate_plot_ax_config(ax)
    return True


def plot_qloq_owd(ax, start_time, qlog_tx_df, qlog_rx_df):
    quic_tx_latency_df = qlog_tx_df[qlog_tx_df['name']
                                    == 'transport:packet_sent'].copy()
    quic_rx_latency_df = qlog_rx_df[qlog_rx_df['name']
                                    == 'transport:packet_received'].copy()

    if quic_tx_latency_df.empty or quic_rx_latency_df.empty:
        return False

    quic_tx_latency_df['ts'] = quic_tx_latency_df['time']
    quic_rx_latency_df['ts'] = quic_rx_latency_df['time']

    return _plot_owd(ax, start_time, quic_tx_latency_df, quic_rx_latency_df, 'data.header.packet_number')


def plot_rtp_owd_log(ax, start_time, rtp_tx_df, rtp_rx_df):
    rtp_tx_latency_df = rtp_tx_df[rtp_tx_df['msg'] == 'rtp packet'].copy()
    rtp_rx_latency_df = rtp_rx_df[rtp_rx_df['msg'] == 'rtp packet'].copy()

    if rtp_tx_latency_df.empty or rtp_rx_latency_df.empty:
        return False

    rtp_tx_latency_df['ts'] = pd.to_datetime(rtp_tx_latency_df['time'])
    rtp_rx_latency_df['ts'] = pd.to_datetime(rtp_rx_latency_df['time'])
    return _plot_owd(ax, start_time, rtp_tx_latency_df, rtp_rx_latency_df, 'rtp-packet.unwrapped-sequence-number')


def _plot_owd(ax, start_time, rtp_tx_latency_df, rtp_rx_latency_df, seq_nr_name):
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


def plot_sctp_stats(ax, start_time, df):
    if df.empty:
        return False

    df = df[df['msg'] == 'pion-sctp-cwnd'].copy()
    if df.empty:
        return False

    df = set_start_time_index(df, start_time, 'time')
    ax.step(df.index, df['cwnd'], where='post',
            label='sctp cwnd', linewidth=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Size')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.legend(loc='upper right')
    return True


def plot_encoding_frame_size(ax, start_time, df):
    encoding = df[df['msg'] == 'encoding frame']
    encoded = df[df['msg'] == 'encoded frame']
    if encoding.empty and encoded.empty:
        return False
    _plot_frame_sizes(ax, encoding, 'raw', encoded, 'encoded')
    return True


def plot_decoding_frame_size(ax, start_time, df):
    decoding = df[df['msg'] == 'decoding frame']
    decoded = df[df['msg'] == 'decoded frame']
    if decoding.empty and decoded.empty:
        return False
    _plot_frame_sizes(ax, decoding, 'encoded', decoded, 'raw')
    return True


def _plot_frame_sizes(ax, df_a, label_a, df_b, label_b):
    df_a = df_a.reset_index()
    df_b = df_b.reset_index()
    ax.bar(df_a.index, df_a['size'], width=0.25, label=label_a)
    ax.bar(df_b.index+0.25, df_b['size'], width=0.25, label=label_b)
    ax.legend(loc='upper right')
    return True


def plot_encoding_time(ax, start_time, df):
    encoding = df[df['msg'] == 'encoding frame']
    encoded = df[df['msg'] == 'encoded frame']
    if encoding.empty and encoded.empty:
        return False
    _plot_encoding_time(ax, encoding, encoded)
    return True


def plot_decoding_time(ax, start_time, df):
    decoding = df[df['msg'] == 'decoding frame']
    decoded = df[df['msg'] == 'decoded frame']
    if decoding.empty and decoded.empty:
        return False
    _plot_encoding_time(ax, decoding, decoded)
    return True


def _plot_encoding_time(ax, df_a, df_b):
    df_a = df_a.reset_index()
    df_b = df_b.reset_index()
    df = pd.merge(df_a, df_b, left_index=True, right_index=True)
    df['latency'] = (pd.to_datetime(df['time_y'])-pd.to_datetime(df['time_x'])) / \
        datetime.timedelta(milliseconds=1) / 1000.0
    ax.bar(df.index, df['latency'], label='Encoding latency')
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
    ax.legend(loc='upper right')


def plot_e2e_latency(ax, start_time, encoding_df, decoding_df):
    encoding = encoding_df[encoding_df['msg']
                           == 'encoding frame'].reset_index()
    decoding = decoding_df[decoding_df['msg']
                           == 'decoded frame'].reset_index()
    if encoding.empty and decoding.empty:
        return False
    df = pd.merge(encoding, decoding, left_index=True, right_index=True)
    df['latency'] = (pd.to_datetime(df['time_y'])-pd.to_datetime(df['time_x'])) / \
        datetime.timedelta(milliseconds=1) / 1000.0
    ax.bar(df.index, df['latency'], label='E2E Latency')
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
    ax.legend(loc='upper right')
    return True


def plot_video_quality(ax, start_time, qm_df):
    ax_psnr = ax
    ax_ssim = ax_psnr.twinx()

    ax_psnr.plot(qm_df["n"], qm_df["psnr_avg"],
                 linestyle="-", marker="", label="psnr avg", color="tab:blue")
    ax_ssim.plot(qm_df["n"], qm_df["ssim_avg"],
                 linestyle="-", marker="", label="ssim avg", color="tab:orange")

    ax_psnr.set_xlabel("Frames")
    ax_psnr.set_ylabel("PSNR (dB)", color="tab:blue")
    ax_ssim.set_ylabel("SSIM", color="tab:orange")

    ax_psnr.tick_params(axis='y', labelcolor="tab:blue")
    ax_ssim.tick_params(axis='y', labelcolor="tab:orange")

    ax_psnr.legend(loc='upper left')
    ax_ssim.legend(loc='upper right')

    ax_psnr.set_xlim(right=3000, left=0)

    return True
