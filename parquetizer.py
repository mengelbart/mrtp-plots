from pathlib import Path

import polars as pl
import subprocess
from io import StringIO


METRICS = [
        'time',
        'exp-config',
        'exp-timestamp',
        'rtt',
        'delivered',
        'loss-target',
        'delay-target',
        'target',
        'rate',
        'interArrivalTime',
        'interDepartureTime',
        'interGroupDelay',
        'estimate',
        'threshold',
        'usage',
        'state',
]

ND_JSON_FILES = {
    'config': 'config.json',
    'tc': 'tc.log',
    'sender_log': 'sender.stderr.log',
    'receiver_log': 'receiver.stderr.log',
}

CSV_FILES = {
    'video_quality': 'video.quality.csv',
    'lost_frames': 'lost_frames.csv',
}


def parse(input, output):
    exp_name = Path(input).name
    exp_time = Path(input).parent.name
    indir = Path(input)

    dfs = {}

    for name, file in ND_JSON_FILES.items():
        if not (indir / file).is_file():
            continue
        dfs[name] = pl.read_ndjson(indir / file).with_columns(
                    pl.lit(exp_name).alias('exp-config'),
                    pl.lit(exp_time).alias('exp-timestamp'),
                )

    for name, file in CSV_FILES.items():
        if not (indir / file).is_file():
            continue
        dfs[name] = pl.read_csv(indir / file).with_columns(
                    pl.lit(exp_name).alias('exp-config'),
                    pl.lit(exp_time).alias('exp-timestamp'),
                )

    for file in indir.iterdir():
        if not Path(file).is_file():
            continue
        if file.is_file() and file.suffix == '.pcap':
            name = file.stem
            dfs[name] = parse_pcap(file).with_columns(
                        pl.lit(exp_name).alias('exp-config'),
                        pl.lit(exp_time).alias('exp-timestamp'),
                    )

    # metrics_df = dfs['sender_log'].filter(
    #     pl.col("msg") != "rtp packet"
    # ).select(METRICS).unpivot(index=[
    #     'time',
    #     'exp-config',
    #     'exp-timestamp',
    # ])
    # packets_df = dfs['sender_log'].filter(
    #     pl.col("msg") == "rtp packet"
    # ).select([
    #     'time',
    #     'exp-config',
    #     'exp-timestamp',
    #     'vantage-point',
    #     'rtp-packet',
    # ])
    # packets_df = packets_df.unnest("rtp-packet")

    out_dir = Path(output)
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    dfs = merge_and_reshape(dfs)
    for name, df in dfs.items():
        df.write_parquet(out_dir / Path(str(name) + '.parquet'))

    return


def parse_pcap(file):
    cmd = [
            'tshark',
            '-r', str(file),
            '-Y', 'rtp and not icmp',
            '-T', 'fields',
            '-E', 'header=y',
            '-E', 'separator=,',
            '-E', 'occurrence=f',
            '-e', 'frame.time_epoch',
            '-e', 'frame.len',
            '-e', 'ip.src',
            '-e', 'udp.srcport',
            '-e', 'ip.dst',
            '-e', 'udp.dstport',
            '-e', 'rtp.ssrc',
            '-e', 'rtp.extseq',
            '-e', 'rtp.timestamp',
            '-e', 'rtp.p_type',
        ]
    out = subprocess.check_output(cmd, text=True)
    df = pl.read_csv(StringIO(out), has_header=True)
    df = df.with_columns([
        (pl.col("frame.time_epoch") *
         1000000).cast(pl.Int64).cast(pl.Datetime('us')).alias('time_epoch'),
        pl.col('rtp.ssrc').str.slice(2)
        .str.to_integer(base=16)
    ])
    name = Path(file).stem
    return df.with_columns([pl.lit(name).alias('pcap')])


def merge_and_reshape(dfs):
    dfs = merge_packets(dfs)
    dfs = reshape_video_quality(dfs)
    dfs = merge_and_reshape_metrics(dfs)
    return dfs


def merge_and_reshape_metrics(dfs):
    fixed_vars = [
        'exp-config',
        'exp-timestamp',
        'time',
        'msg',
    ]
    vars = [
        # 'pts',
        # 'dts',
        # 'duration',
        # 'offset',
        # 'frame-count',
        # 'length',
        'stats',
        # 'address',
        # 'name',
        # 'rtp-timestamp',
        # 'sequence-number',
        # 'unwrapped-sequence-number',
        # 'arrival',
        # 'rate',
        'ccfb-rtt',
    ]
    df = (
        dfs['sender_log']
        # .rename({'RTT': 'ccfb-rtt'})
        .with_columns(
            pl
            .when(pl.col('msg') == 'received ccfb packet report')
            .then(pl.col('RTT').alias('ccfb-rtt'))
        )
        # .with_columns(
        #     pl
        #     .when(pl.col('msg') == 'got scream statistics')
        #     .then(
        #         pl.col('raw')
        #         .str.extract_groups(
        #             r" summary 260747716198400.0  Transmit rate =     0kbps, PLR =  0.00%( 0.00%), CE =  0.00%( 0.00%)[ 0.0%,  0.0%,  0.0%,  0.0%], RTT = 0.050s, Queue delay = 0.000s"
        #         )
        #     )
        #     .otherwise(None)
        #     .alias('scream-stats')
        # )
        .select(fixed_vars + vars)
        # .filter(pl.any_horizontal(pl.col(vars)).is_not_null())
        # .unpivot(
        #     index=fixed_vars,
        #     variable_name='metric',
        # )
    )
    # print(df.filter(pl.col('RTT').is_not_null()).unique(subset='msg'))
    # df = df.unpivot(index=[
    #     'exp-config',
    #     'exp-timestamp',
    #     'time',
    #     'msg',
    # ], variable_name='metric')
    print(df.unique(subset='msg'))
    print(df.columns)
    return dfs


def reshape_video_quality(dfs):
    dfs['video_quality'] = (
            dfs['video_quality'].select([
                'exp-config', 'exp-timestamp',
                'input_file_dist', 'input_file_ref',
                'n', 'mse_avg', 'mse_u', 'mse_v', 'mse_y', 'psnr_avg',
                'psnr_u', 'psnr_v', 'psnr_y', 'ssim_avg', 'ssim_u',
                'ssim_v', 'ssim_y',
            ])
            .unpivot(index=[
                'exp-config', 'exp-timestamp',
                'input_file_dist', 'input_file_ref',
                'n',
            ])
    )
    return dfs


def merge_packets(dfs):
    packet_df_names = ['ns1', 'ns2', 'ns3', 'ns4']
    packet_dfs = {k: v for k, v in dfs.items() if k in packet_df_names}
    ns_keys = ['ip.src', 'ip.dst', 'rtp.ssrc', 'rtp.extseq']
    for name in packet_dfs:
        cols = ['exp-config', 'exp-timestamp', 'time_epoch', 'ip.src',
                'udp.srcport', 'ip.dst', 'udp.dstport', 'rtp.ssrc',
                'rtp.extseq', 'rtp.timestamp', 'rtp.p_type', 'pcap']
        cols = cols if name == 'ns4' else ns_keys
        packet_dfs[name] = (
                dfs[name].select(
                    *cols,
                    pl.col('time_epoch').alias(f'time_{name}')
                )
        )

    cols = ['exp-config', 'exp-timestamp', 'time',
            'marker', 'sequence-number', 'rtp.extseq',
            'timestamp', 'rtp.ssrc', 'payload-length']
    tx_rx_keys = ['rtp.ssrc', 'rtp.extseq']
    tx_df = prepare_tx_rx_dfs(dfs['sender_log'], 'tx', cols)
    rx_df = prepare_tx_rx_dfs(dfs['receiver_log'], 'rx', tx_rx_keys)

    # merge backwards, since we send from ns4 to ns1
    df = (
        tx_df
        .join(packet_dfs['ns4'], on=tx_rx_keys, how='left', suffix='_ns4',
              validate='1:1')
        .join(packet_dfs['ns3'], on=ns_keys, how='left', suffix='_ns3',
              validate='1:1')
        .join(packet_dfs['ns2'], on=ns_keys, how='left', suffix='_ns2',
              validate='1:1')
        .join(packet_dfs['ns1'], on=ns_keys, how='left', suffix='_ns1',
              validate='1:1')
        .join(rx_df, on=tx_rx_keys, how='left', suffix='_rx', validate='1:1')
    )

    dfs['packets'] = df
    for name in packet_df_names:
        dfs.pop(name)

    return dfs


def prepare_tx_rx_dfs(df, name, cols):
    df = (df.filter(
            pl.col('msg') == 'rtp packet'
        )
    )
    df = (
        df
        .select([c for c in df.columns if df[c].null_count() < df.height])
        .unnest('rtp-packet')
        .rename({
            'unwrapped-sequence-number': 'rtp.extseq',
            'ssrc': 'rtp.ssrc',
        })
        .select(*cols, pl.col('time').alias(f'time_{name}'))
        .with_columns((pl.col('rtp.extseq') + 65536).alias('rtp.extseq'))
    )
    return df
