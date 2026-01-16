import datetime
import re
import matplotlib.ticker as mticker
import pandas as pd

import video_quality

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

_RTP_FOW_IDS = {0, 10, 20}


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
        f'dtls plotting only works for the namespaces ns1 and ns4, and not for {namespace}')


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
    plot_target_rate(ax, start_time, tx_df, color='tab:green')
    plot_rtp_rate_logging(ax, start_time, tx_df, 'tx')
    plot_rtp_rate_logging(ax, start_time, rx_df, 'rx')
    _rate_plot_ax_config(ax)
    return True


def _plot_rtp_send_rate_pcaps(ax, start_time, sender_ip, rtp_tx_df, name='tx'):
    rtp_tx_df = rtp_tx_df[rtp_tx_df['src'] == sender_ip].copy()
    rtp_tx_df['rate'] = rtp_tx_df['length'] * 8
    return _plot_rate(ax, start_time, rtp_tx_df, name)


def _plot_rtp_recv_rate_pcaps(ax, start_time, receiver_ip, rtp_rx_df, name='rx'):
    rtp_rx_df = rtp_rx_df[rtp_rx_df['dst'] == receiver_ip].copy()
    rtp_rx_df['rate'] = rtp_rx_df['length'] * 8
    return _plot_rate(ax, start_time, rtp_rx_df, name)


def plot_rtp_rates_pcaps(ax, start_time, cap_df, tx_log_df, rtp_tx_df, rtp_rx_df, config_df):
    """plots rtp rates from pcaps"""
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(ax, start_time, tx_log_df, color='tab:green')

    sender_ip, receiver_ip = _get_ips_from_config(config_df)
    _plot_rtp_send_rate_pcaps(ax, start_time, sender_ip, rtp_tx_df)
    _plot_rtp_recv_rate_pcaps(ax, start_time, receiver_ip, rtp_rx_df)

    _rate_plot_ax_config(ax)
    return True


def plot_quic_rates(ax, start_time, cap_df, tx_log_df, qlog_tx_df, qlog_rx_df):
    """plots quic rates from qlogs"""

    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(ax, start_time, tx_log_df, color='tab:green')

    quic_tx_latency_df = qlog_tx_df[qlog_tx_df['name']
                                    == 'transport:packet_sent'].copy()
    quic_rx_latency_df = qlog_rx_df[qlog_rx_df['name']
                                    == 'transport:packet_received'].copy()

    if quic_tx_latency_df.empty or quic_rx_latency_df.empty:
        return False

    quic_tx_latency_df['rate'] = quic_tx_latency_df['data.raw.payload_length'] * 8
    _plot_data_rate(ax, start_time, quic_tx_latency_df, 'tx')

    quic_rx_latency_df['rate'] = quic_rx_latency_df['data.raw.length'] * 8
    _plot_data_rate(ax, start_time, quic_rx_latency_df, 'rx')

    _rate_plot_ax_config(ax)
    return True


def _plot_data_media_sum_rate(ax, data_df, media_df):
    if data_df.empty:
        ax.plot(media_df.index,
                media_df['rate'], label='total', linewidth=0.5)
        return

    combined_df = data_df.join(
        media_df, how='outer', lsuffix='_data', rsuffix='_media')
    combined_df['rate'] = combined_df.get(
        'rate_data', 0) + combined_df.get('rate_media', 0)
    ax.plot(combined_df.index,
            combined_df['rate'], label='total', linewidth=0.5, color='tab:purple')


def plot_all_send_rates(ax, start_time, cap_df, tx_df):
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(
        ax, start_time, tx_df, event_name='NEW_TARGET_RATE', label='tr all', color='tab:green')
    plot_target_rate(ax, start_time, tx_df,
                     label='tr media', color='tab:red')

    _, media_df = plot_rtp_rate_logging(ax, start_time, tx_df, 'media')
    _, data_df = plot_data_rate(ax, start_time, tx_df, 'data')

    _plot_data_media_sum_rate(ax, data_df, media_df)
    _rate_plot_ax_config(ax)
    return True


def plot_all_recv_rates(ax, start_time, cap_df, tx_df, rx_df):
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(
        ax, start_time, tx_df, event_name='NEW_TARGET_RATE', color='tab:green')

    _, media_df = plot_rtp_rate_logging(ax, start_time, rx_df, 'media')
    _, data_df = plot_data_rate(
        ax, start_time, rx_df, 'data', event_name='DataSink received data')

    _plot_data_media_sum_rate(ax, data_df, media_df)
    _rate_plot_ax_config(ax)
    return True


def plot_all_send_rates_pcaps(ax, start_time, cap_df, tx_df, rtp_tx_df, dtls_tx_df, config_df, only_flow_rates=False):
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(ax, start_time, tx_df,
                     label='tr media', color='tab:green')
    plot_target_rate(
        ax, start_time, tx_df, event_name='NEW_TARGET_RATE', label='tr all', color='tab:green')

    sender_ip, _ = _get_ips_from_config(config_df)

    _, media_df = _plot_rtp_send_rate_pcaps(
        ax, start_time, sender_ip, rtp_tx_df, name='media')
    _, data_df = _plot_dlts_send_rate(
        ax, start_time, sender_ip, dtls_tx_df, name='data')

    if not only_flow_rates:
        _plot_data_media_sum_rate(ax, data_df, media_df)

    _rate_plot_ax_config(ax)
    return True


def plot_all_send_rates_and_owd_pcaps_nodtls(axs, start_time, cap_df, tx_df, rtp_tx_df, rtp_rx_df, config_df):
    return plot_all_send_rates_and_owd_pcaps(axs, start_time, cap_df, tx_df, rtp_tx_df, rtp_rx_df, pd.DataFrame(), config_df)


def plot_all_send_rates_and_owd_pcaps(axs, start_time, cap_df, tx_df, rtp_tx_df, rtp_rx_df, dtls_tx_df, config_df):
    rate_plotted = plot_all_send_rates_pcaps(
        axs[0], start_time, cap_df, tx_df, rtp_tx_df, dtls_tx_df, config_df, only_flow_rates=True)
    owd_plotted = plot_rtp_owd_pcap(axs[1], start_time, rtp_tx_df, rtp_rx_df)
    return rate_plotted or owd_plotted


def plot_all_send_rates_and_loss_pcaps_nodtls(axs, start_time, cap_df, tx_df, rtp_tx_df, rtp_rx_df, config_df):
    return plot_all_send_rates_and_loss_pcaps(axs, start_time, cap_df, tx_df, rtp_tx_df, rtp_rx_df, pd.DataFrame(), config_df)


def plot_all_send_rates_and_loss_pcaps(axs, start_time, cap_df, tx_df, rtp_tx_df, rtp_rx_df, dtls_tx_df, config_df):
    rate_plotted = plot_all_send_rates_pcaps(
        axs[0], start_time, cap_df, tx_df, rtp_tx_df, dtls_tx_df, config_df, only_flow_rates=True)
    loss_plotted = _plot_loss_count(
        axs[1], start_time, rtp_tx_df, rtp_rx_df, 'extseq')
    return rate_plotted or loss_plotted


def plot_rtp_rates_and_owd_quic(axs, start_time, cap_df, tx_log_df, rx_log_df, qlog_tx_df, qlog_rx_df, roq_df):
    rate_plotted = plot_all_send_rates_qlog(
        axs[0], start_time, cap_df, tx_log_df, rx_log_df, qlog_tx_df, roq_df, only_flow_rates=True)
    owd_plotted = _plot_qlog_owd_per_flow(
        axs[1], start_time, qlog_tx_df, qlog_rx_df, roq_df)
    return rate_plotted or owd_plotted


def plot_rtp_rates_and_loss_quic(axs, start_time, cap_df, tx_log_df, rx_log_df, qlog_tx_df, qlog_rx_df, roq_df):
    rate_plotted = plot_all_send_rates_qlog(
        axs[0], start_time, cap_df, tx_log_df, rx_log_df, qlog_tx_df, roq_df, only_flow_rates=True)
    owd_plotted = _plot_rtp_loss_count_quic(
        axs[1], start_time, qlog_tx_df, qlog_rx_df, roq_df)
    return rate_plotted or owd_plotted


def plot_send_rates_and_owd_quic(axs, start_time, cap_df, tx_log_df, rx_log_df, qlog_tx_df, qlog_rx_df):
    rate_plotted = _plot_send_rate_quic(
        axs[0], start_time, cap_df, tx_log_df, rx_log_df, qlog_tx_df, qlog_rx_df)
    owd_plotted = plot_qlog_owd(axs[1], start_time, qlog_tx_df, qlog_rx_df)
    return rate_plotted or owd_plotted


def plot_send_rates_and_loss_quic(axs, start_time, cap_df, tx_log_df, rx_log_df, qlog_tx_df, qlog_rx_df):
    rate_plotted = _plot_send_rate_quic(
        axs[0], start_time, cap_df, tx_log_df, rx_log_df, qlog_tx_df, qlog_rx_df)
    owd_plotted = _plot_loss_count_quic(
        axs[1], start_time, qlog_tx_df, qlog_rx_df)
    return rate_plotted or owd_plotted


def _plot_send_rate_quic(ax, start_time, cap_df, tx_log_df, rx_log_df, qlog_tx_df, qlog_rx_df):
    plot_capacity(ax, start_time, cap_df)

    quic_tx_latency_df = qlog_tx_df[qlog_tx_df['name']
                                    == 'transport:packet_sent'].copy()
    if quic_tx_latency_df.empty:
        return False

    quic_tx_latency_df['rate'] = quic_tx_latency_df['data.raw.length'] * 8
    _plot_data_rate(ax, start_time, quic_tx_latency_df, 'quic')
    _rate_plot_ax_config(ax)
    return True


def plot_all_recv_rates_pcaps(ax, start_time, cap_df, tx_df, rtp_rx_df, dtls_rx_df, config_df):
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(
        ax, start_time, tx_df, event_name='NEW_TARGET_RATE', color='tab:green')

    _, receiver_ip = _get_ips_from_config(config_df)

    _, data_df = _plot_dlts_recv_rate(
        ax, start_time, receiver_ip, dtls_rx_df, name='data')
    _, media_df = _plot_rtp_recv_rate_pcaps(
        ax, start_time, receiver_ip, rtp_rx_df, name='media')

    _plot_data_media_sum_rate(ax, data_df, media_df)
    _rate_plot_ax_config(ax)
    return True


def _plot_all_qlog_rates(ax, start_time, cap_df, tx_df, rx_df, quic_df, roq_df, only_flow_rates=False):
    plot_capacity(ax, start_time, cap_df)
    # if not only_flow_rates:
    plot_target_rate(
        ax, start_time, tx_df, event_name='NEW_TARGET_RATE', label='tr all', color='tab:green')
    plot_target_rate(ax, start_time, tx_df, label='tr media', color='tab:red')

    # get frames
    qlog_frames = _explode_qlog_frames(quic_df)

    roq_stream_mapping = roq_df[roq_df['name'] == 'roq:stream_opened']
    if roq_stream_mapping.empty:
        return False

    # plot each RTP flow separately
    rtp_streams_mapping = roq_stream_mapping[roq_stream_mapping['data.flow_id'].isin(
        _RTP_FOW_IDS)]

    if rtp_streams_mapping.empty:
        return False

    media_dfs = []
    for flow_id in sorted(rtp_streams_mapping['data.flow_id'].unique()):
        flow_mapping = rtp_streams_mapping[rtp_streams_mapping['data.flow_id'] == flow_id]
        rtp_tx = qlog_frames.merge(
            flow_mapping, left_on='stream_id', right_on='data.stream_id', suffixes=['', '_mapping'])
        rtp_tx['rate'] = rtp_tx['length'] * 8
        name = 'media'
        if len(media_dfs) > 1:
            name = f'media flow {int(flow_id)}'
        plotted, media_df = _plot_data_rate(ax, start_time, rtp_tx, name)
        if plotted:
            media_dfs.append(media_df)

    if not media_dfs:
        return False

    # plot data stream
    data_df = pd.DataFrame()
    dc_stream_mapping = rx_df[rx_df['msg'] == 'new dc stream']
    if not dc_stream_mapping.empty:
        data_streams_mapping = dc_stream_mapping[dc_stream_mapping['flowID'] == 3]

        if not data_streams_mapping.empty:
            data_tx = qlog_frames.merge(
                data_streams_mapping, left_on='stream_id', right_on='streamID', suffixes=['', '_mapping'])

            # length is length field of the frame
            data_tx['rate'] = data_tx['length'] * 8

            _, data_df = _plot_data_rate(ax, start_time, data_tx, 'data')

    # sum media rates across all RTP flows
    if not only_flow_rates:
        media_sum_df = pd.concat(media_dfs).groupby(
            level=0).sum(numeric_only=True)
        _plot_data_media_sum_rate(ax, data_df, media_sum_df)

    _rate_plot_ax_config(ax)
    return True


def _explode_qlog_frames(qlog_df):
    frames_df = qlog_df.explode('data.frames')
    frames_normalized = pd.json_normalize(frames_df['data.frames'], sep='.')
    frames_normalized.index = frames_df.index
    return pd.concat(
        [frames_df.drop('data.frames', axis=1), frames_normalized], axis=1)


def _plot_qlog_owd_per_flow(ax, start_time, qlog_tx_df, qlog_rx_df, roq_df):
    quic_tx_latency_df = qlog_tx_df[qlog_tx_df['name']
                                    == 'transport:packet_sent'].copy()
    quic_rx_latency_df = qlog_rx_df[qlog_rx_df['name']
                                    == 'transport:packet_received'].copy()

    if quic_tx_latency_df.empty or quic_rx_latency_df.empty:
        return False

    # get frames
    tx_qlog_frames = _explode_qlog_frames(qlog_tx_df)
    rx_qlog_frames = _explode_qlog_frames(qlog_rx_df)

    stream_mapping = roq_df[roq_df['name'] == 'roq:stream_opened']
    if stream_mapping.empty:
        return False

    # plot each RTP flow separately
    rtp_streams_mapping = stream_mapping[stream_mapping['data.flow_id'].isin(
        _RTP_FOW_IDS)]

    flow_ids = rtp_streams_mapping['data.flow_id'].unique()
    for flow_id in sorted(flow_ids):
        flow_mapping = rtp_streams_mapping[rtp_streams_mapping['data.flow_id'] == flow_id]
        rtp_tx = tx_qlog_frames.merge(
            flow_mapping, left_on='stream_id', right_on='data.stream_id', suffixes=['', '_mapping'])
        rtp_rx = rx_qlog_frames.merge(
            flow_mapping, left_on='stream_id', right_on='data.stream_id', suffixes=['', '_mapping'])
        rtp_tx['ts'] = rtp_tx['time']
        rtp_rx['ts'] = rtp_rx['time']

        name = 'media'
        if len(flow_ids) > 1:
            name = f'media flow {int(flow_id)}'
        _plot_owd(ax, start_time, rtp_tx, rtp_rx,
                  'data.header.packet_number', label=name)

    return True


def plot_all_send_rates_qlog(ax, start_time, cap_df, tx_df, rx_df, qlog_tx_df, roq_df, only_flow_rates=False):
    quic_tx_df = qlog_tx_df[qlog_tx_df['name']
                            == 'transport:packet_sent'].copy()
    if quic_tx_df.empty:
        return False
    return _plot_all_qlog_rates(ax, start_time, cap_df, tx_df, rx_df, quic_tx_df, roq_df, only_flow_rates=only_flow_rates)


def plot_all_recv_rates_qlog(ax, start_time, cap_df, tx_df, rx_df, qlog_rx_df, roq_df):
    qlog_rx_df = qlog_rx_df[qlog_rx_df['name']
                            == 'transport:packet_received'].copy()
    if qlog_rx_df.empty:
        return False
    return _plot_all_qlog_rates(ax, start_time, cap_df, tx_df, rx_df, qlog_rx_df, roq_df)


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


def plot_target_rate(ax, start_time, df, event_name='NEW_TARGET_MEDIA_RATE', label='target', color='black'):
    df = df[df['msg'] == event_name].copy()
    if df.empty:
        return False
    df = set_start_time_index(df, start_time, 'time')
    ax.plot(df.index, df['rate'], label=label, linewidth=0.5, color=color)
    return True


def plot_rtp_rate_logging(ax, start_time, df, label):
    df = df[df['msg'] == 'rtp packet'].copy()
    if df.empty:
        return False, df
    df['rate'] = df['rtp-packet.payload-length'] * 8
    return _plot_data_rate(ax, start_time, df, label)


def plot_data_rate(ax, start_time, df, label, event_name='DataSource sent data'):
    df = df[df['msg'] == event_name].copy()
    if df.empty:
        return False, df
    df['rate'] = df['payload-length'] * 8
    return _plot_data_rate(ax, start_time, df, label)


def _plot_rate(ax, start_time, df, label):
    """time as index and rate as column"""

    df['second'] = (df.index - start_time).total_seconds()
    df['second'] = df['second'].astype(int)  # Round to nearest second
    df_grouped = df.groupby('second')['rate'].sum().reset_index()

    # Only plot if there's data
    if not df_grouped.empty:
        # Fill in zeros
        first_second = df_grouped['second'].min()
        last_second = df_grouped['second'].max()
        all_seconds = pd.DataFrame(
            {'second': range(int(first_second), int(last_second) + 1)})
        df_grouped = all_seconds.merge(df_grouped, on='second', how='left')
        df_grouped['rate'] = df_grouped['rate'].fillna(0)

        df_grouped.set_index('second', inplace=True)
        ax.plot(df_grouped.index,
                df_grouped['rate'], label=label, linewidth=0.5)
        return True, df_grouped

    return True, pd.DataFrame()


def _plot_data_rate(ax, start_time, df, label):
    df.set_index('time', inplace=True)
    return _plot_rate(ax, start_time, df, label)


def plot_rtp_loss_rate_pcap(ax, start_time, rtp_tx_df, rtp_rx_df):
    return _plot_rtp_loss_rate(ax, start_time, rtp_tx_df, rtp_rx_df, 'extseq')


def plot_rtp_loss_rate_log(ax, start_time, rtp_tx_df, rtp_rx_df):
    """rtp loss without jitter buffer"""
    rtp_tx_df = rtp_tx_df[rtp_tx_df['msg'] == 'rtp packet'].copy()
    rtp_rx_df = rtp_rx_df[rtp_rx_df['msg'] == 'rtp packet'].copy()
    if rtp_tx_df.empty:
        return False

    return _plot_rtp_loss_rate(ax, start_time, rtp_tx_df, rtp_rx_df, 'rtp-packet.sequence-number')


def _plot_rtp_loss_count_quic(ax, start_time, qlog_tx_df, qlog_rx_df, roq_df):
    """rtp loss without jitter buffer"""
    quic_tx_df = qlog_tx_df[qlog_tx_df['name']
                            == 'transport:packet_sent'].copy()
    quic_rx_df = qlog_rx_df[qlog_rx_df['name']
                            == 'transport:packet_received'].copy()

    if quic_tx_df.empty or quic_rx_df.empty:
        return False

    stream_mapping = roq_df[roq_df['name'] == 'roq:stream_opened']
    if stream_mapping.empty:
        return False

    # get flowID streamID mapping
    rtp_streams_mapping = stream_mapping[stream_mapping['data.flow_id'].isin(
        _RTP_FOW_IDS)]

    if rtp_streams_mapping.empty:
        return False

    # explodes each frame in its own row
    qlog_tx_frames = _explode_qlog_frames(qlog_tx_df)
    qlog_rx_frames = _explode_qlog_frames(qlog_rx_df)

    # select correct streamIDs and groupBy packet_number, so we do count packet loss and not each frame separately
    qlog_tx_filtered = qlog_tx_frames[qlog_tx_frames['stream_id'].isin(
        rtp_streams_mapping['data.stream_id'])].groupby('data.header.packet_number').first()
    qlog_rx_filtered = qlog_rx_frames[qlog_rx_frames['stream_id'].isin(
        rtp_streams_mapping['data.stream_id'])].groupby('data.header.packet_number').first()

    return _plot_loss_count(ax, start_time, qlog_tx_filtered, qlog_rx_filtered, "data.header.packet_number")


def _plot_loss_count_quic(ax, start_time, qlog_tx_df, qlog_rx_df):
    """rtp loss without jitter buffer"""
    quic_tx_df = qlog_tx_df[qlog_tx_df['name']
                            == 'transport:packet_sent'].copy()
    quic_rx_df = qlog_rx_df[qlog_rx_df['name']
                            == 'transport:packet_received'].copy()

    if quic_tx_df.empty or quic_rx_df.empty:
        return False

    return _plot_loss_count(ax, start_time, quic_tx_df, quic_rx_df, "data.header.packet_number")


def plot_rtp_full_loss_rate_log(ax, start_time, rtp_tx_df, rtp_rx_df):
    """rtp loss with jitter buffer"""
    rtp_tx_df = rtp_tx_df[rtp_tx_df['msg'] == 'rtp to pts mapping'].copy()
    rtp_rx_df = rtp_rx_df[rtp_rx_df['msg'] == 'rtp to pts mapping'].copy()
    if rtp_tx_df.empty:
        return False

    return _plot_rtp_loss_rate(ax, start_time, rtp_tx_df, rtp_rx_df, 'unwrapped-sequence-number')


def _plot_rtp_loss_rate(ax, start_time, rtp_tx_df, rtp_rx_df, seq_nr_name):
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


def _plot_loss_count(ax, start_time, rtp_tx_df, rtp_rx_df, seq_nr_name):
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

    merged_df['second'] = (merged_df.index - start_time).total_seconds()
    merged_df.set_index('second', inplace=True)

    ax.plot(merged_df.index, merged_df['lost'], linewidth=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Lost packets')
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:.0f}'))
    return True


def plot_rtp_owd_pcap(ax, start_time, rtp_tx_df, rtp_rx_df):
    rtp_tx_latency_df = rtp_tx_df.copy()
    rtp_rx_latency_df = rtp_rx_df.copy()
    rtp_tx_latency_df['ts'] = rtp_tx_df.index
    rtp_rx_latency_df['ts'] = rtp_rx_df.index
    return _plot_owd(ax, start_time, rtp_tx_latency_df, rtp_rx_latency_df, 'extseq', label='media')


def get_rtp_owd_pcap_df(start_time, rtp_tx_df, rtp_rx_df):
    rtp_tx_latency_df = rtp_tx_df.copy()
    rtp_rx_latency_df = rtp_rx_df.copy()
    rtp_tx_latency_df['ts'] = rtp_tx_df.index
    rtp_rx_latency_df['ts'] = rtp_rx_df.index
    return _merge_owd(start_time, rtp_tx_latency_df, rtp_rx_latency_df, 'extseq')


def plot_rtp_owd_pcap_cdf(ax, start_time, rtp_tx_df, rtp_rx_df):
    df = get_rtp_owd_pcap_df(start_time, rtp_tx_df, rtp_rx_df)
    ax.ecdf(df['latency'], label='rtp')
    ax.set_xlabel("latency (ms)")
    ax.set_ylabel("CDF")
    return True


def plot_dtls_owd(ax, start_time, dtls_tx_df, dtls_rx_df, config_df):
    sender_ip, receiver_ip = _get_ips_from_config(config_df)

    dtls_tx_latency_df = dtls_tx_df[dtls_tx_df['src'] == sender_ip].copy()
    dtls_rx_latency_df = dtls_rx_df[dtls_rx_df['dst'] == receiver_ip].copy()
    dtls_tx_latency_df['ts'] = dtls_tx_latency_df.index
    dtls_rx_latency_df['ts'] = dtls_rx_latency_df.index

    return _plot_owd(ax, start_time, dtls_tx_latency_df, dtls_rx_latency_df, 'seq')


def plot_dtls_loss(ax, start_time, dtls_tx_df, dtls_rx_df, config_df):
    sender_ip, receiver_ip = _get_ips_from_config(config_df)

    dtls_tx_latency_df = dtls_tx_df[dtls_tx_df['src'] == sender_ip].copy()
    dtls_rx_latency_df = dtls_rx_df[dtls_rx_df['dst'] == receiver_ip].copy()

    return _plot_rtp_loss_rate(ax, start_time, dtls_tx_latency_df, dtls_rx_latency_df, 'seq')


def _plot_dlts_send_rate(ax, start_time, sender_ip, dtls_tx_df, name='tx'):
    if dtls_tx_df.empty:
        return False, pd.DataFrame()

    dtls_tx_df = dtls_tx_df[dtls_tx_df['src'] == sender_ip].copy()
    dtls_tx_df['rate'] = dtls_tx_df['length'] * 8
    return _plot_rate(ax, start_time, dtls_tx_df, name)


def _plot_dlts_recv_rate(ax, start_time, receiver_ip, dtls_rx_df, name='rx'):
    dtls_rx_df = dtls_rx_df[dtls_rx_df['dst'] == receiver_ip].copy()
    dtls_rx_df['rate'] = dtls_rx_df['length'] * 8
    return _plot_rate(ax, start_time, dtls_rx_df, name)


def plot_dtls_rates(ax, start_time, cap_df, tx_df, dtls_tx_df, dtls_rx_df, config_df):
    plot_capacity(ax, start_time, cap_df)
    plot_target_rate(ax, start_time, tx_df, color='tab:green')

    sender_ip, receiver_ip = _get_ips_from_config(config_df)

    _plot_dlts_send_rate(ax, start_time, sender_ip, dtls_tx_df)
    _plot_dlts_recv_rate(ax, start_time, receiver_ip, dtls_rx_df)

    _rate_plot_ax_config(ax)
    return True


def plot_qlog_owd(ax, start_time, qlog_tx_df, qlog_rx_df):
    quic_tx_latency_df = qlog_tx_df[qlog_tx_df['name']
                                    == 'transport:packet_sent'].copy()
    quic_rx_latency_df = qlog_rx_df[qlog_rx_df['name']
                                    == 'transport:packet_received'].copy()

    if quic_tx_latency_df.empty or quic_rx_latency_df.empty:
        return False

    quic_tx_latency_df['ts'] = quic_tx_latency_df['time']
    quic_rx_latency_df['ts'] = quic_rx_latency_df['time']
    return _plot_owd(ax, start_time, quic_tx_latency_df, quic_rx_latency_df, 'data.header.packet_number')


def get_qlog_owd_df(start_time, qlog_tx_df, qlog_rx_df):
    quic_tx_latency_df = qlog_tx_df[qlog_tx_df['name']
                                    == 'transport:packet_sent'].copy()
    quic_rx_latency_df = qlog_rx_df[qlog_rx_df['name']
                                    == 'transport:packet_received'].copy()

    if quic_tx_latency_df.empty or quic_rx_latency_df.empty:
        return False, pd.DataFrame()

    quic_tx_latency_df['ts'] = quic_tx_latency_df['time']
    quic_rx_latency_df['ts'] = quic_rx_latency_df['time']
    return True, _merge_owd(start_time, quic_tx_latency_df,
                            quic_rx_latency_df,  'data.header.packet_number')


def plot_qlog_owd_cdf(ax, start_time, qlog_tx_df, qlog_rx_df):
    ok, df = get_qlog_owd_df(start_time, qlog_tx_df, qlog_rx_df)
    if not ok:
        return False

    ax.ecdf(df['latency'], label='quic')
    ax.set_xlabel("latency (ms)")
    ax.set_ylabel("CDF")
    return True


def plot_rtp_owd_log_udp(ax, start_time, rtp_tx_df, rtp_rx_df, pcap_tx_df, pcap_rx_df, config_df, stacked=True):
    """ for udp and webrtc transport"""

    tx_mapping = rtp_tx_df[rtp_tx_df['msg'] == 'rtp to pts mapping'].copy()
    rx_mapping = rtp_rx_df[rtp_rx_df['msg'] == 'rtp to pts mapping'].copy()
    if tx_mapping.empty or rx_mapping.empty:
        return False

    # tx: mapping-log <-> pcap -> sender stack delay
    tx_mapping['ts'] = tx_mapping['time']
    tx_mapping['extseq'] = tx_mapping['unwrapped-sequence-number'].astype(
        'int64')
    tx_mapping['extseq'] = tx_mapping['extseq'] + 65536

    sender_ip, receiver_ip = _get_ips_from_config(config_df)

    tx_pcap = pcap_tx_df[pcap_tx_df['src'] == sender_ip].copy()
    tx_pcap['ts'] = tx_pcap.index

    send_stack_delay = _merge_owd(start_time, tx_mapping,
                                  tx_pcap, 'extseq')
    send_stack_delay['send_stack_delay'] = send_stack_delay['latency']

    # rx: pcap <-> mapping-log -> receiver stack delay
    rx_mapping['ts'] = rx_mapping['time']
    rx_mapping['extseq'] = rx_mapping['unwrapped-sequence-number'].astype(
        'int64')
    rx_mapping['extseq'] = rx_mapping['extseq'] + 65536

    rx_pcap = pcap_rx_df[pcap_rx_df['dst'] == receiver_ip].copy()
    rx_pcap['ts'] = rx_pcap.index

    recv_stack_delay = _merge_owd(start_time, rx_pcap, rx_mapping,
                                  'extseq')
    recv_stack_delay['recv_stack_delay'] = recv_stack_delay['latency']

    print("len recv delay: ", len(recv_stack_delay))

    # tx pcaps -> rx pcaps -> network delay
    network_delay = _merge_owd(start_time, tx_pcap, rx_pcap, 'extseq')
    network_delay['network_delay'] = network_delay['latency']

    # combine all delays

    # Set extseq as index before joining
    send_stack_delay = send_stack_delay.reset_index().set_index('extseq')
    recv_stack_delay = recv_stack_delay.reset_index().set_index('extseq')
    network_delay = network_delay.reset_index().set_index('extseq')

    combined_df = send_stack_delay.join(
        recv_stack_delay, how='inner', lsuffix='', rsuffix='_recv')
    combined_df = combined_df.join(
        network_delay, how='inner', lsuffix='', rsuffix='_network')

    if stacked:
        ax.stackplot(combined_df['second'],
                     combined_df['send_stack_delay'],
                     combined_df['network_delay'],
                     combined_df['recv_stack_delay'],
                     labels=['send stack', 'network', 'recv stack'],
                     alpha=0.7)
    else:
        combined_df['total_latency'] = (combined_df['send_stack_delay'] +
                                        combined_df['network_delay'] +
                                        combined_df['recv_stack_delay'])
        ax.plot(combined_df['second'], combined_df['total_latency'],
                label='latency', linewidth=0.5)

    ax.legend()

    _plot_owd_settings(ax)
    return True


def plot_rtp_owd_log_udp_overall(ax, start_time, rtp_tx_df, rtp_rx_df, pcap_tx_df, pcap_rx_df, config_df):
    return plot_rtp_owd_log_udp(ax, start_time, rtp_tx_df, rtp_rx_df,
                                pcap_tx_df, pcap_rx_df, config_df, stacked=False)


def plot_rtp_owd_log_roq(ax, start_time, rtp_tx_df, rtp_rx_df, quic_tx_df, stacked=True):
    """ for roq transport. quic_tx_df not used but makes sure it is only called for roq transport"""

    tx_mapping = rtp_tx_df[rtp_tx_df['msg'] == 'rtp to pts mapping'].copy()
    if tx_mapping.empty:
        return False
    if 'flow-id' in tx_mapping.columns:
        flow_ids = tx_mapping['flow-id'].unique()
        if len(flow_ids) > 1:
            return False

    rtp_tx_log = rtp_tx_df[rtp_tx_df['msg'] == 'rtp packet'].copy()
    rtp_rx_log = rtp_rx_df[rtp_rx_df['msg'] == 'rtp packet'].copy()
    rx_mapping = rtp_rx_df[rtp_rx_df['msg'] == 'rtp to pts mapping'].copy()

    if tx_mapping.empty or rtp_tx_log.empty or rtp_rx_log.empty or rx_mapping.empty:
        return False

    tx_mapping['ts'] = tx_mapping['time']
    tx_mapping['extseq'] = tx_mapping['unwrapped-sequence-number'].astype(
        'int64')

    rtp_tx_log['ts'] = rtp_tx_log['time']
    rtp_tx_log['extseq'] = rtp_tx_log['rtp-packet.sequence-number'].astype(
        'int64')

    rx_mapping['ts'] = rx_mapping['time']
    rx_mapping['extseq'] = rx_mapping['unwrapped-sequence-number'].astype(
        'int64')

    rtp_rx_log['ts'] = rtp_rx_log['time']
    rtp_rx_log['extseq'] = rtp_rx_log['rtp-packet.sequence-number'].astype(
        'int64')

    quic_stack_network = _merge_owd(start_time, rtp_tx_log,
                                    rtp_rx_log, 'extseq')
    quic_stack_network['net_quic_delay'] = quic_stack_network['latency']

    recv_stack_delay = _merge_owd(start_time, rtp_rx_log, rx_mapping,
                                  'extseq')
    recv_stack_delay['recv_stack_delay'] = recv_stack_delay['latency']

    send_stack_delay = _merge_owd(start_time, tx_mapping,
                                  rtp_tx_log, 'extseq')
    send_stack_delay['send_stack_delay'] = send_stack_delay['latency']

    send_stack_delay = send_stack_delay.reset_index().set_index('extseq')
    recv_stack_delay = recv_stack_delay.reset_index().set_index('extseq')
    quic_stack_network = quic_stack_network.reset_index().set_index('extseq')

    combined_df = quic_stack_network.join(
        recv_stack_delay, how='inner', lsuffix='', rsuffix='_recv')
    combined_df = combined_df.join(
        send_stack_delay, how='inner', lsuffix='_send', rsuffix='')

    if stacked:
        ax.stackplot(combined_df['second'],
                     combined_df['send_stack_delay'],
                     combined_df['net_quic_delay'],
                     combined_df['recv_stack_delay'],
                     labels=['send stack', 'quic stack + network', 'recv stack'],
                     alpha=0.7)
    else:
        combined_df['total_latency'] = (combined_df['send_stack_delay'] +
                                        combined_df['net_quic_delay'] +
                                        combined_df['recv_stack_delay'])
        ax.plot(combined_df['second'], combined_df['total_latency'],
                label='latency', linewidth=0.5)

    ax.legend()
    _plot_owd_settings(ax)
    return True


def plot_rtp_owd_log_roq_overall(ax, start_time, rtp_tx_df, rtp_rx_df, quic_tx_df):
    return plot_rtp_owd_log_roq(ax, start_time, rtp_tx_df,
                                rtp_rx_df, quic_tx_df, stacked=False)


def _merge_owd(start_time, rtp_tx_latency_df, rtp_rx_latency_df, seq_nr_name):
    merged_df = rtp_tx_latency_df.merge(rtp_rx_latency_df, on=seq_nr_name)
    merged_df['latency'] = (merged_df['ts_y'] - merged_df['ts_x']) / \
        datetime.timedelta(milliseconds=1) / 1000.0
    df = set_start_time_index(merged_df, start_time, 'ts_x')
    return df


def _plot_owd(ax, start_time, rtp_tx_latency_df, rtp_rx_latency_df, seq_nr_name, label='Latency'):
    if rtp_tx_latency_df.empty or rtp_rx_latency_df.empty:
        return False
    df = _merge_owd(start_time, rtp_tx_latency_df,
                    rtp_rx_latency_df, seq_nr_name)
    ax.plot(df.index, df['latency'], label=label, linewidth=0.5, linestyle='')
    _plot_owd_settings(ax)
    return True


def _plot_owd_settings(ax):
    # ax.set_ylim(bottom=0, top=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Latency')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
    # ax.grid(True, axis='y', linestyle='--', alpha=0.3)
    ax.legend(loc='upper right')


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


def plot_frame_latency(ax, start_time, tx_df, rx_df):
    try:
        tx_merged = video_quality.map_frames_sender_pipeline(tx_df)
        rx_merged = video_quality.map_frames_receiver_pipeline(rx_df)
    except (KeyError, ValueError):
        return False

    merged_df = tx_merged.merge(
        rx_merged, on='rtp-timestamp_mapping', suffixes=('_tx', '_rx'))

    # group on time_ori and time_frame_rx and aggregate
    grouped_df = merged_df.groupby(['time_ori', 'time_frame_rx']).agg(
        frames_count=('rtp-timestamp_mapping', 'count')
    ).reset_index()

    grouped_df['latency'] = (
        pd.to_datetime(grouped_df['time_frame_rx']) - pd.to_datetime(grouped_df['time_ori'])) / \
        datetime.timedelta(milliseconds=1) / 1000.0

    df = set_start_time_index(grouped_df, start_time, 'time_ori')
    ax.plot(df.index, df['latency'], label='Latency', linewidth=0.5)
    # ax.set_ylim(bottom=0, top=0.5)
    ax.set_xlabel('Time')
    ax.set_ylabel('Latency')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
    ax.grid(True, axis='y', linestyle='--', alpha=0.3)
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

    ax_psnr.grid(True, axis='y', linestyle='--', alpha=0.3, color="tab:blue")

    ax_psnr.legend(loc='upper left')
    ax_ssim.legend(loc='upper right')

    return True


def plot_video_quality_psnr_cdf(ax, _, qm_df):
    ax.ecdf(qm_df["psnr_avg"], label="psnr avg")
    ax.set_xlabel("PSNR")
    ax.set_ylabel("CDF")
    ax.set_ylim([0, 1])
    ax.legend(loc='lower right')
    return True


def plot_video_quality_ssim_cdf(ax, _, qm_df):
    ax.ecdf(qm_df["ssim_avg"], label="ssim avg")
    ax.set_xlabel("SSIM")
    ax.set_ylabel("CDF")
    ax.set_ylim([0, 1])
    ax.legend(loc='lower right')
    return True


def plot_video_rate(ax, start_time, rx_df):
    rx_data = rx_df[rx_df['msg'] == 'encoder src'].copy()
    if rx_data.empty:
        return False

    if 'flow-id' in rx_data.columns:
        flow_ids = sorted(rx_data['flow-id'].unique())
        if len(flow_ids) > 1:
            # Plot each flow separately
            for flow_id in flow_ids:
                flow_data = rx_data[rx_data['flow-id'] == flow_id].copy()
                flow_data['rate'] = flow_data['length'] * 8
                _plot_data_rate(ax, start_time, flow_data,
                                f'video rate flow {int(flow_id)}')
        else:
            rx_data['rate'] = rx_data['length'] * 8
            _plot_data_rate(ax, start_time, rx_data, 'video rate')
    else:
        rx_data['rate'] = rx_data['length'] * 8
        _plot_data_rate(ax, start_time, rx_data, 'video rate')

    _rate_plot_ax_config(ax)
    return True


def plot_frame_size_dist(ax, start_time, tx_df):
    rx_data = tx_df[tx_df['msg'] == 'encoder src'].copy()
    if rx_data.empty:
        return False

    rx_data = rx_data.reset_index()

    # histogram
    n, bins, patches = ax.hist(rx_data['length'], bins=50, edgecolor='white',
                               color='steelblue', alpha=0.8, label='Frame Size Distribution')

    # labels for bars
    for i in range(len(patches)):
        if n[i] > 0:  # Only label non-zero bars
            ax.text(patches[i].get_x() + patches[i].get_width()/2,
                    patches[i].get_height(),
                    f'{int(n[i])}',
                    ha='center', va='bottom', fontsize=6)

    ax.set_xlabel('Size')
    ax.set_ylabel('Count')
    ax.xaxis.set_major_formatter(mticker.EngFormatter(unit='B'))
    ax.legend(loc='upper right')

    return True


def plot_frame_size(ax, start_time, tx_df):
    rx_data = tx_df[tx_df['msg'] == 'encoder src'].copy()
    if rx_data.empty:
        return False

    rx_data = set_start_time_index(rx_data, start_time, 'time')

    ax.scatter(rx_data.index, rx_data['length'],
               label='Frame Size', s=8, marker='.')

    ax.set_xlabel('Time')
    ax.set_ylabel('Size')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='B'))
    ax.legend(loc='upper right')
    ax.grid(True, axis='y', linestyle='--', alpha=0.3)

    return True


def plot_frame_size_and_tr(axs, start_time, cap_df, tx_df, rx_df):
    plot_capacity(axs[0], start_time, cap_df)
    plot_target_rate(
        axs[0], start_time, tx_df, label='tr all', color='tab:green')

    plot_frame_size(axs[1], start_time, tx_df)

    _rate_plot_ax_config(axs[0])
    return True


def plot_file_completion(axs, start_time, tx_df, rx_df):
    tx_df = tx_df[tx_df['msg'] == 'DataSrc Chunk started']
    rx_df = rx_df[rx_df['msg'] == 'DataSink Chunk finished']

    if tx_df.empty or rx_df.empty:
        return False

    merged_df = tx_df.merge(rx_df, on='chunk-number',
                            suffixes=('_start', '_finish'))
    if merged_df.empty:
        return False

    merged_df['completion_time'] = (
        pd.to_datetime(merged_df['time_finish']) -
        pd.to_datetime(merged_df['time_start'])
    ) / datetime.timedelta(seconds=1)

    avg_completion_time = merged_df['completion_time'].mean()
    axs.axhline(y=avg_completion_time, color='red', linestyle='--', linewidth=1,
                label=f'Avg: {avg_completion_time:.2f}s')

    axs.bar(merged_df['chunk-number'], merged_df['completion_time'], width=0.5,
            label='Chunk Completion Time')

    # labels for bars
    for i, (chunk_num, comp_time) in enumerate(zip(merged_df['chunk-number'], merged_df['completion_time'])):
        axs.text(chunk_num, comp_time, f'{comp_time:.1f}s',
                 ha='center', va='bottom', fontsize=9)

    axs.set_xlabel('Chunk Number')
    axs.set_ylabel('Completion Time (s)')
    axs.yaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
    axs.legend(loc='upper right')

    return True
