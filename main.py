#!/usr/bin/env python

import argparse
import asyncio

import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import parsers
import plotters
import html_generator
import serializers
import plot_version_comparison
import video_quality

plots = [
    ('RTP Rates (logging)', plotters.plot_rtp_rates_log, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather'], 'rtp_rates_logs.png'),
    ('RTP Rates (pcaps)', plotters.plot_rtp_rates_pcaps, [
     'tc.feather', 'sender.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'config.feather'], 'rtp_rates.png'),
    ('Send Rates (logging)', plotters.plot_all_send_rates, [
     'tc.feather', 'sender.stderr.feather'], 'all_send_rates.png'),
    ('Receive Rates (logging)', plotters.plot_all_recv_rates, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather'], 'all_recv_rates.png'),
    # ('RTP Send Rate', plotters.plot_rtp_rate, [
    #  'sender.stderr.feather'], 'rtp_send_rate.png'),
    # ('RTP Recv Rate', plotters.plot_rtp_rate, [
    #  'receiver.stderr.feather'], 'rtp_recv_rate.png'),
    ('RTP Loss Rate (pcap)', plotters.plot_rtp_loss_pcap, ['ns4.rtp.feather',
     'ns1.rtp.feather'], 'rtp_loss.png'),
    ('RTP Loss Rate (logging)', plotters.plot_rtp_loss_log, ['sender.stderr.feather',
     'receiver.stderr.feather'], 'rtp_loss_log.png'),
    ('RTP OWD (pcap)', plotters.plot_rtp_owd_pcap, ['ns4.rtp.feather',
     'ns1.rtp.feather'], 'rtp_owd.png'),
    ('RTP OWD (logging)', plotters.plot_rtp_owd_log, ['sender.stderr.feather',
     'receiver.stderr.feather'], 'rtp_owd_log.png'),
    ('QUIC OWD (logging)', plotters.plot_qloq_owd, ['sender.feather',
     'receiver.feather'], 'quic_owd.png'),
    ('SCReAM Queue Delay', plotters.plot_scream_queue_delay,
     ['sender.stderr.feather'], 'scream_queue_delay.png'),
    ('SCReAM CWND', plotters.plot_scream_cwnd, [
     'sender.stderr.feather'], 'scream_cwnd.png'),
    ('GCC RTT', plotters.plot_gcc_rtt, [
     'sender.stderr.feather'], 'gcc_rtt.png'),
    ('GCC Target Rates', plotters.plot_gcc_target_rates, [
        'sender.stderr.feather'], 'gcc_target_rates.png'),
    ('GCC Estimates', plotters.plot_gcc_estimates, [
     'sender.stderr.feather'], 'gcc_estimates.png'),
    ('GCC Usage and State', plotters.plot_gcc_usage_and_state,
     ['sender.stderr.feather'], 'gcc_usage_state.png'),
    ('SCTP Stats', plotters.plot_sctp_stats,
     ['sender.stderr.sctp.feather'], 'sctp_stats.png'),
    ('DLTS OWD (pcap)', plotters.plot_dlts_owd, ['ns4.dtls.feather',
     'ns1.dtls.feather', 'config.feather'], 'dtls_owd.png'),
    ('DLTS loss (pcap)', plotters.plot_dlts_loss, ['ns4.dtls.feather',
     'ns1.dtls.feather', 'config.feather'], 'dtls_loss.png'),
    ('DLTS rate (pcap)', plotters.plot_dlts_rates, [
        'tc.feather', 'sender.stderr.feather', 'ns4.dtls.feather', 'ns1.dtls.feather',
        'config.feather'], 'dtls_rate.png'),

    ('Encoding frame sizes', plotters.plot_encoding_frame_size, [
     'sender.stderr.feather'], 'encoding_frame_sizes.png'),
    ('Decoding frame sizes', plotters.plot_decoding_frame_size, [
        'receiver.stderr.feather'], 'receiver_frame_sizes.png'),

    ('Encoding time', plotters.plot_encoding_time, [
     'sender.stderr.feather'], 'encoding_time.png'),
    ('Decoding time', plotters.plot_decoding_time, [
     'receiver.stderr.feather'], 'decoding_time.png'),

    ('E2E Latency', plotters.plot_e2e_latency, [
     'sender.stderr.feather', 'receiver.stderr.feather'], 'e2e_latency.png'),
    ('Video Quality Metrics', plotters.plot_video_quality, [
     'video.quality.feather'], 'video_quality.png'),
]


async def parse_file(input, out_dir):
    path = Path(input)
    if path.name in ['config.json', 'tc.log', 'receiver.stderr.log', 'sender.stderr.log']:
        df = parsers.parse_json_log(input)
        serializers.write_feather(
            df, Path(out_dir) / Path(input).with_suffix('.feather').name)
    if path.name in ['sender.qlog', 'receiver.qlog']:
        df = parsers.parse_qlog(input)
        serializers.write_feather(
            df, Path(out_dir) / Path(input).with_suffix('.feather').name)
    if path.name in ['sender.stderr.log']:
        df = parsers.parse_pion_sctp_log(input)
        serializers.write_feather(
            df, Path(out_dir) / Path(input).with_suffix('.sctp.feather').name)
    if path.suffix == '.pcap':
        rtp, rtcp, dtls = await parsers.parse_pcap(input)

        if not rtp.empty:
            serializers.write_feather(
                rtp, Path(out_dir) / Path(Path(input).stem + '.rtp.feather'))
        if not rtcp.empty:
            serializers.write_feather(
                rtcp, Path(out_dir) / Path(Path(input).stem + '.rtcp.feather'))
        if not dtls.empty:
            serializers.write_feather(
                dtls, Path(out_dir) / Path(Path(input).stem + '.dtls.feather'))
    if path.name in ['video.quality.csv']:
        df = pd.read_csv(input)
        serializers.write_feather(
            df, Path(out_dir) / Path(Path(input).stem + '.feather'))


async def parse_all_cmd(args):
    dir = Path(args.input)
    for file in dir.iterdir():
        if file.is_file():
            await parse_file(file, args.output)


async def parse_cmd(args):
    await parse_file(args.input, args.output)


async def plot_cmd(args):
    config_feather = Path(args.input) / Path('config.feather')
    config = serializers.read_feather(config_feather)
    start_time = pd.Timestamp(config['time'][0])

    for title, func, files, out_name in plots:
        fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(8, 3))
        paths = [Path(args.input) / Path(f) for f in files]
        if all(p.is_file() for p in paths):
            dfs = [serializers.read_feather(p) for p in paths]
            plotted = func(ax, start_time, *dfs)
            if not plotted:
                print(f'dropping empty plot {func.__name__}')
                plt.close(fig)
                continue
        else:
            missing = [str(p) for p in paths if not p.is_file()]
            print(
                f'skipping plot {func.__name__} due to missing dependencies {', '.join(missing)}')
            continue
        ax.set_title(title)
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(Path(args.output) / Path(out_name), dpi=300)
        plt.close(fig)


async def generate_cmd(args):
    html_generator.generate_html(args.input)


async def plot_combis_cmd(args):
    plot_version_comparison.plot_version_comparison(args.input, args.output)


async def calc_video_metrics(args):
    video_quality.calculate_quality_metrics(
        args.reference, args.distorted, args.output)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(help='sub-command help', required=True)

    parse = subparsers.add_parser(
        'parse', help='parses log files, converts them to a data frame and serializes the data frame as feather file(s)')
    parse.add_argument('-i', '--input', help='input log file', required=True)
    parse.add_argument(
        '-o', '--output', help='output directory', required=True)
    parse.set_defaults(func=parse_cmd)

    parse_all = subparsers.add_parser(
        'parse-all', help='parse all files in a directory by trying to find a suitable parser for each file by name')
    parse_all.add_argument(
        '-i', '--input', help='input directory', required=True)
    parse_all.add_argument(
        '-o', '--output', help='output directory', required=True)
    parse_all.set_defaults(func=parse_all_cmd)

    plot = subparsers.add_parser(
        'plot', help='reads a data frame from a feather file and creates plots')
    plot.add_argument(
        '-i', '--input', help='input directory', required=True)
    plot.add_argument(
        '-o', '--output', help='output directory', required=True)
    plot.set_defaults(func=plot_cmd)

    generate = subparsers.add_parser(
        'generate', help='generates a HTML site to show results')

    generate.set_defaults(func=generate_cmd)
    generate.add_argument(
        '-i', '--input', help='input directory', required=True)

    plot_combis = subparsers.add_parser(
        'plot-combis', help='creates a combined plot for each test case')
    plot_combis.add_argument(
        '-i', '--input', help='input directory with test cases', required=True)
    plot_combis.add_argument(
        '-o', '--output', help='output directory for plots', required=True)
    plot_combis.set_defaults(func=plot_combis_cmd)

    video_qm = subparsers.add_parser(
        'video-quality', help='caluculate video quality metrics using ffmpeg')
    video_qm.add_argument(
        '-r', '--reference', help='reference video', required=True)
    video_qm.add_argument(
        '-d', '--distorted', help='distorted video', required=True)
    video_qm.add_argument(
        '-o', '--output', help='output directory for result csv', required=True)
    video_qm.set_defaults(func=calc_video_metrics)

    args = parser.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
