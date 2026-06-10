from pathlib import Path
from datetime import datetime

import polars as pl
import subprocess
import glob
import json
from io import StringIO


# pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_cols(50)


class Parquetizer:
    def __init__(self, input, output):
        self.input = Path(input)
        self.output = Path(output)
        self.dfs = {}
        self.pcap_dfs = {}
        self.qlog_dfs = {}
        self.rtp_tx = None
        self.rtp_rx = None

    def parquetize(self):
        print(f'parquetizing {self.input} to {self.output}')
        self.read_config()
        self.read_tc()
        self.read_video_quality_log()
        self.read_lost_frames_log()
        self.read_sender_log()
        self.read_receiver_log()
        self.read_sim_log()
        self.read_qlog()
        for ns in ['ns1', 'ns2', 'ns3', 'ns4']:
            file = self.input / f'{ns}.pcap'
            if file.is_file():
                self.pcap_dfs[Path(file).stem] = parse_pcap(file)

        if self.rtp_tx is not None:
            self.build_rtp_packets_df()

        self.build_metrics()

        self.save()

    def save(self):
        dfs = {
            **self.dfs,
            **self.qlog_dfs,
        }
        print(f'saving dfs: {dfs.keys()}')
        out_dir = Path(self.output)
        Path(out_dir).mkdir(parents=True, exist_ok=True)
        for name, df in dfs.items():
            if df is not None and not df.is_empty():
                df.write_parquet(out_dir / f'{name}.parquet')

    def read_config(self):
        file = self.input / 'config.json'
        with open(file) as f:
            self.config = json.load(f)
        self.dfs['config'] = (
                pl
                .read_ndjson(file)
                .with_columns(
                    pl.col('time').str.to_datetime('%+')
                )
            )
        self.name = self.config['name']
        self.netconf, self.appconf = self.config['name'].split('_')
        self.timestamp = datetime.fromisoformat(self.config['time'])

    def read_tc(self):
        self.dfs['tc'] = (
            pl.read_ndjson(self.input / 'tc.log')
            .with_columns(
                pl.lit(self.name).alias('name'),
                pl.lit(self.netconf).alias('netconf'),
                pl.lit(self.appconf).alias('appconf'),
            )
            .with_columns(
                pl.col('time').str.to_datetime('%+')
            )
            .with_columns(
                pl.col('bandwidth').str.extract_groups(r'(\d+)([a-zA-Z]+)')
            )
            .with_columns(
                pl.col('bandwidth')
                .struct.field('1').cast(pl.Int64).alias('bw-value'),
                pl.col('bandwidth')
                .struct.field('2').alias('bw-unit')
            )
            .with_columns(
                pl.when(pl.col('bw-unit') == 'bit').then(1)
                .when(pl.col('bw-unit') == 'kbit').then(1_000)
                .when(pl.col('bw-unit') == 'mbit').then(1_000_000)
                .when(pl.col('bw-unit') == 'gbit').then(1_000_000_000)
                .otherwise(0)
                .alias('bw-multiplier')
            )
            .with_columns(
                (pl.col('bw-value') *
                 pl.col('bw-multiplier')).alias('bandwidth'),
            )
            .drop('bw-value')
            .drop('bw-unit')
            .drop('bw-multiplier')
            .drop('traffic_control')
            .unpivot(
                index=['name', 'netconf', 'appconf', 'time'],
                on=['bandwidth', 'burst', 'limit', 'delay'],
            )
        )

    def read_sim_log(self):
        # workaround for simulation logs, as sender and receiver logs are in the same file
        # will be removed in the future when simulation produces separate sender and receiver logs
        rtp_send_points = ['roq sink', 'webrtc-send']
        rtp_recv_points = ['roq src', 'webrtc-recv']

        if not (self.input / 'sim.stderr.log').is_file():
            return
        
        # sender
        lines = read_json_lines(self.input / 'sim.stderr.log')
        self.sender_log = (
                pl.from_dicts(lines, infer_schema_length=1000)
                .with_columns(
                    pl.lit(self.name).alias('name'),
                    pl.lit(self.netconf).alias('netconf'),
                    pl.lit(self.appconf).alias('appconf'),
                    pl.col('time').str.to_datetime('%+'),
                )
            )
        df = self.sender_log.filter((pl.col('msg') == 'rtp packet') & (
           pl.col('vantage-point').is_in(rtp_send_points)))
        if 'rtp-packet' in df.columns:
            self.rtp_tx = read_rtp_packets_from_stderr(df)

        # receiver
        receiver_log = (
                pl.from_dicts(lines, infer_schema_length=50000)
                .with_columns(
                    pl.lit(self.name).alias('name'),
                    pl.lit(self.netconf).alias('netconf'),
                    pl.lit(self.appconf).alias('appconf'),
                    pl.col('time').str.to_datetime('%+'),
                )
            )
        df = receiver_log.filter((pl.col('msg') == 'rtp packet') & (
           pl.col('vantage-point').is_in(rtp_recv_points)))
        if 'rtp-packet' in df.columns:
            self.rtp_rx = read_rtp_packets_from_stderr(df)

        df = receiver_log.filter(pl.col('msg') == 'DataSink read')
        if not df.is_empty():
            self.dfs['data_rx'] = read_data_from_stderr(df)

    def read_sender_log(self):
        # check if we have sim results
        if (self.input / 'sim.stderr.log').is_file():
            return
        lines = read_json_lines(self.input / 'sender.stderr.log')
        self.sender_log = (
                pl.from_dicts(lines, infer_schema_length=1000)
                .with_columns(
                    pl.lit(self.name).alias('name'),
                    pl.lit(self.netconf).alias('netconf'),
                    pl.lit(self.appconf).alias('appconf'),
                    pl.col('time').str.to_datetime('%+'),
                )
            )
        df = self.sender_log.filter(pl.col('msg') == 'rtp packet')
        if 'rtp-packet' in df.columns:
            self.rtp_tx = read_rtp_packets_from_stderr(df)

    def read_receiver_log(self):
        # check if we have sim results
        if (self.input / 'sim.stderr.log').is_file():
            return
        lines = read_json_lines(self.input / 'receiver.stderr.log')
        receiver_log = (
                pl.from_dicts(lines, infer_schema_length=50000)
                .with_columns(
                    pl.lit(self.name).alias('name'),
                    pl.lit(self.netconf).alias('netconf'),
                    pl.lit(self.appconf).alias('appconf'),
                    pl.col('time').str.to_datetime('%+'),
                )
            )
        df = receiver_log.filter(pl.col('msg') == 'rtp packet')
        if 'rtp-packet' in df.columns:
            self.rtp_rx = read_rtp_packets_from_stderr(df)

        df = receiver_log.filter(pl.col('msg') == 'DataSink read')
        if not df.is_empty():
            self.dfs['data_rx'] = read_data_from_stderr(df)

    def read_video_quality_log(self):
        if not (self.input / 'video.quality.csv').is_file():
            return
        self.dfs['video_quality'] = (
                pl
                .read_csv(self.input / 'video.quality.csv')
                .with_columns(
                    pl.lit(self.name).alias('name'),
                    pl.lit(self.netconf).alias('netconf'),
                    pl.lit(self.appconf).alias('appconf'),
                )
                # reorder columns
                .select(
                    'name', 'netconf', 'appconf', 'n', 'mse_avg', 'mse_u',
                    'mse_v', 'mse_y', 'psnr_avg', 'psnr_u', 'psnr_v',
                    'psnr_y', 'ssim_avg', 'ssim_u', 'ssim_v', 'ssim_y',
                    'input_file_dist', 'input_file_ref'
                )
                .unpivot(index=[
                    'name', 'netconf', 'appconf', 'input_file_dist',
                    'input_file_ref', 'n',
                ])
            )

    def read_lost_frames_log(self):
        if not (self.input / 'lost_frames.csv').is_file():
            return
        self.dfs['lost_frames'] = (
                pl.read_csv(self.input / 'lost_frames.csv')
                .with_columns(
                    pl.lit(self.name).alias('name'),
                    pl.lit(self.netconf).alias('netconf'),
                    pl.lit(self.appconf).alias('appconf'),
                )
                .select(
                    'name', 'netconf', 'appconf', 'frame_number',
                    'rtp_timestamp',
                )
            )

    def read_qlog(self):
        qlog_files = glob.glob(f'{self.input}/*.sqlog')
        print(f'reading qlog files: {qlog_files}')
        for f in qlog_files:
            path = Path(f)
            if not (path).is_file():
                continue
            df = read_qlog(path)
            packets_df = (
                df
                .filter(
                    pl.col('name').is_in(['transport:packet_sent',
                                          'transport:packet_received'])
                )
            )
            packets_df = (
                packets_df
                .select([c for c in packets_df.columns if
                         packets_df[c].null_count() < packets_df.height])
            )
            metrics_df = (
                df
                .filter(pl.col('name') == 'recovery:metrics_updated')
            )
            metrics_df = (
                metrics_df
                .select([c for c in metrics_df.columns if
                         metrics_df[c].null_count() < metrics_df.height])
                .unpivot(
                    index=['time', 'name'],
                )
                .drop_nulls()
            )
            name = f.split('_')[-1]
            self.qlog_dfs[f'qlog-{Path(name).stem}-packets'] = packets_df
            self.qlog_dfs[f'qlog-{Path(name).stem}-metrics'] = metrics_df

    def build_rtp_packets_df(self):
        df = self.rtp_tx
        keys = ['rtp.ssrc', 'rtp.extseq']
        if self.rtp_rx is not None and not self.rtp_rx.is_empty():
            rtp_rx = self.rtp_rx.select(*keys, 'time')
            df = (
                df
                .join(rtp_rx, on=keys, how='left', suffix='_rx',
                      validate='1:1')
            )
        for ns, ns_df in self.pcap_dfs.items():
            if (ns_df is not None and not ns_df.is_empty()
                    and all([c in ns_df.columns for c in keys])):
                ns_df = ns_df.select(*keys, 'time')
                df = (
                    df
                    .join(ns_df, on=keys, how='left', suffix=f'_{ns}',
                          validate='1:1')
                )
        self.dfs['rtp_packets'] = df

    def build_metrics(self):
        fixed_vars = ['name', 'netconf', 'appconf', 'time']
        self.dfs['metrics'] = pl.DataFrame(schema={
            'name': pl.String,
            'netconf': pl.String,
            'appconf': pl.String,
            'time': pl.Datetime('us', 'UTC'),
            'metric': pl.String,
            'value': pl.Float64,
        })
        # filter target rate log messages
        target_rate = (
                self
                .sender_log.filter(pl.col('msg') == 'NEW_TARGET_MEDIA_RATE')
            )
        if 'rate' in target_rate.columns and not target_rate.is_empty():
            target_rate = (
                    target_rate
                    .rename({'rate': 'target-rate'})
                    .with_columns(
                        pl.col(['target-rate'])
                        .cast(pl.Float64)
                    )
                    .unpivot(
                        index=fixed_vars,
                        on=['target-rate'],
                        variable_name='metric',
                    )
                )
            self.dfs['metrics'] = pl.concat([self.dfs['metrics'], target_rate])

        # filter gcc log messages
        if 'gcc' in self.name:
            gcc = (
                self.sender_log
                .with_columns(
                    pl
                    .col('msg').str.extract_groups(
                        r'rtt=(\d+), delivered=(\d+), lossTarget=(\d+), '
                        r'delayTarget=(\d+), target=(\d+)'
                    )
                    .alias('gcc-rate-groups')
                )
                .with_columns(
                    pl.col('gcc-rate-groups')
                    .struct.field('1').cast(pl.Float64)
                    .alias('gcc-rtt'),
                    pl.col('gcc-rate-groups')
                    .struct.field('2').cast(pl.Float64).
                    alias('gcc-delivered'),
                    pl.col('gcc-rate-groups')
                    .struct.field('3').cast(pl.Float64)
                    .alias('gcc-loss-target'),
                    pl.col('gcc-rate-groups')
                    .struct.field('4').cast(pl.Float64)
                    .alias('gcc-delay-target'),
                    pl.col('gcc-rate-groups')
                    .struct.field('5').cast(pl.Float64)
                    .alias('gcc-target'),
                )
                .filter(pl.col('gcc-rtt').is_not_null())
                .drop('gcc-rate-groups')
                .unpivot(
                    index=fixed_vars,
                    on=['gcc-rtt', 'gcc-delivered', 'gcc-loss-target',
                        'gcc-delay-target', 'gcc-target'],
                    variable_name='metric',
                )
            )
            self.dfs['metrics'] = pl.concat([self.dfs['metrics'], gcc])
        if 'scream' in self.name:
            scream = (
                self.sender_log
                .filter(pl.col('msg') == 'got scream statistics')
                .with_columns(
                    pl.col('stats').str.extract_groups(
                        r'\s+summary (\d+.\d+)\s+'
                        r'Transmit rate =\s+(\d+.+),\s+'
                        r'PLR =\s+(\d+\.\d+)%\(\s+(\d+.\d+)%\), '
                        r'CE =\s+(\d+.\d+)%\(\s+(\d+.\d+)%\)'
                        r'\[\s*'
                        r'(\d+.\d+)%,\s+(\d+.\d+)%,\s+(\d+.\d+)%,\s+(\d+.\d+)%'
                        r'\],\s+'
                        r'RTT =\s+(\d+.\d+)s,\s+'
                        r'Queue delay =\s+(\d+.\d+)s.*'
                    )
                    .struct.rename_fields(['ssrc', 'tr-rate', 'loss1',
                                           'loss2', 'ce1', 'ce2', 'ce3', 'ce4',
                                           'ce5', 'ce6', 'scream_rtt',
                                           'scream_queue_delay'])
                )
                .unnest('stats')
                .with_columns(
                    pl.col(['ssrc', 'loss1', 'loss2', 'ce1', 'ce2', 'ce3',
                            'ce4', 'ce5', 'ce6', 'scream_rtt',
                            'scream_queue_delay'])
                    .cast(pl.Float64)
                )
                .with_columns(
                    pl.col('tr-rate')
                    .str.extract_groups(r'(\d+)([a-zA-Z]+)')
                )
                .with_columns(
                    pl.col('tr-rate').struct.field('1').cast(pl.Float64)
                    .alias('tr-rate-value'),
                    pl.col('tr-rate').struct.field('2')
                    .alias('tr-rate-unit')
                )
                .with_columns(
                    pl.when(pl.col('tr-rate-unit') == 'bps').then(1)
                    .when(pl.col('tr-rate-unit') == 'kbps').then(1_000)
                    .when(pl.col('tr-rate-unit') == 'mbps').then(1_000_000)
                    .when(pl.col('tr-rate-unit') == 'gbps').then(1_000_000_000)
                    .otherwise(0)
                    .alias('tr-rate-multiplier')
                )
                .with_columns(
                    (pl.col('tr-rate-value') *
                     pl.col('tr-rate-multiplier')).alias('transmit-rate'),
                )
                .drop('tr-rate-value')
                .drop('tr-rate-unit')
                .drop('tr-rate-multiplier')
                .drop('level')
                .drop('msg')
                .unpivot(
                    index=['time', 'name', 'netconf', 'appconf'],
                    on=['transmit-rate', 'loss1', 'loss2', 'ce1', 'ce2', 'ce3',
                        'ce4', 'ce5', 'ce6', 'scream_rtt',
                        'scream_queue_delay'],
                    variable_name='metric',
                )
                .select(['name', 'netconf', 'appconf', 'time', 'metric',
                         'value'])
            )
            self.dfs['metrics'] = pl.concat([self.dfs['metrics'], scream])


def read_json_lines(file):
    data = []
    with open(file) as f:
        for line in f:
            if not line:
                continue
            try:
                data.append(json.loads(line.strip()))
            except json.JSONDecodeError:
                # print(f'failed to decode json: {e}, line="{line}"')
                continue
    return data


def read_rtp_packets_from_stderr(df):
    return (
        df
        .select([c for c in df.columns if df[c].null_count() <
                 df.height])
        .unnest('rtp-packet')
        .rename({
            'unwrapped-sequence-number': 'rtp.extseq',
            'ssrc': 'rtp.ssrc',
        })
        .select(
            'time', 'name', 'netconf', 'appconf', 'marker',
            'rtp.extseq', 'timestamp', 'rtp.ssrc', 'payload-length',
        )
        .with_columns(
            (pl.col('rtp.extseq') + 65536)
        )
    )


def read_data_from_stderr(df):
    return (
        df
        .select([c for c in df.columns if df[c].null_count() <
                 df.height])
        .select(
            'time', 'name', 'netconf', 'appconf', 'bytes-read',
        )
    )

def parse_pcap(file):
    cmd = [
            'tshark',
            '-r', str(file),
            '-Y', 'rtp and not icmp and not quic',
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
    if df.is_empty():
        return None
    df = (
        df.with_columns(
            [
                (pl.col("frame.time_epoch") * 1000000)
                .cast(pl.Int64)
                .cast(pl.Datetime('us', time_zone='UTC'))
                .alias('time'),
                pl.col('rtp.ssrc').str.slice(2)
                .str.to_integer(base=16)
            ]
        )
        .drop('frame.time_epoch')
        .select([
                    'time', 'ip.src', 'udp.srcport', 'ip.dst',
                    'udp.dstport', 'frame.len', 'rtp.ssrc', 'rtp.extseq',
                    'rtp.timestamp',
                ])
    )
    name = Path(file).stem
    return df.insert_column(1, (pl.lit(name)).alias('pcap'))


def read_qlog(file):
    lines = read_json_lines(file)
    df = pl.json_normalize(lines)
    reference_time = (
            df[0]['trace.common_fields.reference_time.wall_clock_time']
            .str.to_datetime('%+')
    )
    df = (
        df.with_columns(
            (pl.col('time').cast(pl.Duration('ms')) +
             reference_time).alias('time')
        )
    )
    return df
