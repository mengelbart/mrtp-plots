#!/usr/bin/env python

import argparse
import asyncio

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
    # RTP rates
    # ('RTP Rates (logging)', plotters.plot_rtp_rates_log, 1,1, [
    #  'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather'], 'rtp_rates_logs.png'),
    ('RTP Network Rates (pcaps)', plotters.plot_rtp_rates_pcaps, 1, 1, [
     'tc.feather', 'sender.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'config.feather'], 'rtp_rates.png'),
    ('QUIC Network Rates (qlog)', plotters.plot_quic_rates, 1, 1, [
     'tc.feather', 'sender.stderr.feather', 'sender.feather', 'receiver.feather'], 'quic_rates.png'),
    # ('RTP Send Rate', plotters.plot_rtp_rate, 1,1, [
    #  'sender.stderr.feather'], 'rtp_send_rate.png'),
    # ('RTP Recv Rate', plotters.plot_rtp_rate, 1,1, [
    #  'receiver.stderr.feather'], 'rtp_recv_rate.png'),

    # combined rates
    ('Send Rates (logging)', plotters.plot_all_send_rates, 1, 1, [
     'tc.feather', 'sender.stderr.feather'], 'all_send_rates.png'),
    ('Receive Rates (logging)', plotters.plot_all_recv_rates, 1, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather'], 'all_recv_rates.png'),
    ('Send Rates (pcap)', plotters.plot_all_send_rates_pcaps, 1, 1, [
     'tc.feather', 'sender.stderr.feather', 'ns4.rtp.feather', 'ns4.dtls.feather', 'config.feather'], 'all_send_rates_pcaps.png'),
    ('Receive Rates (pcap)', plotters.plot_all_recv_rates_pcaps, 1, 1, [
     'tc.feather', 'sender.stderr.feather', 'ns1.rtp.feather', 'ns1.dtls.feather', 'config.feather'], 'all_recv_rates_pcaps.png'),
    ('Send Rates (qlog)', plotters.plot_all_send_rates_qlog, 1, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather'], 'all_send_rates_qlog.png'),
    ('Receive Rates (qlog)', plotters.plot_all_recv_rates_qlog, 1, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'receiver.feather'], 'all_recv_rates_qlog.png'),

    # loss
    ('RTP Network Loss Rate (pcap)', plotters.plot_rtp_loss_rate_pcap, 1, 1, ['ns4.rtp.feather',
     'ns1.rtp.feather'], 'rtp_loss.png'),
    ('RTP Loss Rate (network)', plotters.plot_rtp_loss_rate_log, 1, 1, ['sender.stderr.feather',
     'receiver.stderr.feather'], 'rtp_loss_net.png'),
    ('RTP Loss Rate (after jitter)', plotters.plot_rtp_full_loss_rate_log, 1, 1, ['sender.stderr.feather',
     'receiver.stderr.feather'], 'rtp_loss_full.png'),

    # OWD
    ('Network OWD (RTP pcap)', plotters.plot_rtp_owd_pcap, 1, 1, ['ns4.rtp.feather',
     'ns1.rtp.feather'], 'rtp_owd.png'),
    ('RTP OWD', plotters.plot_rtp_owd_log_udp, 1, 1, ['sender.stderr.feather',
     'receiver.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'config.feather'], 'rtp_owd_log.png'),
    ('RTP OWD', plotters.plot_rtp_owd_log_roq, 1, 1, ['sender.stderr.feather',
     'receiver.stderr.feather', 'sender.feather'], 'rtp_owd_quic.png'),
    ('Network OWD (QUIC qlog)', plotters.plot_qlog_owd, 1, 1, ['sender.feather',
     'receiver.feather'], 'quic_owd.png'),

    # DTLS
    ('DTLS OWD (pcap)', plotters.plot_dtls_owd, 1, 1, ['ns4.dtls.feather',
     'ns1.dtls.feather', 'config.feather'], 'dtls_owd.png'),
    ('DTLS loss (pcap)', plotters.plot_dtls_loss, 1, 1, ['ns4.dtls.feather',
     'ns1.dtls.feather', 'config.feather'], 'dtls_loss.png'),
    ('DTLS rate (pcap)', plotters.plot_dtls_rates, 1, 1, [
        'tc.feather', 'sender.stderr.feather', 'ns4.dtls.feather', 'ns1.dtls.feather',
        'config.feather'], 'dtls_rate.png'),

    # CC stats
    ('SCReAM Queue Delay', plotters.plot_scream_queue_delay, 1, 1,
     ['sender.stderr.feather'], 'scream_queue_delay.png'),
    ('SCReAM CWND', plotters.plot_scream_cwnd, 1, 1, [
     'sender.stderr.feather'], 'scream_cwnd.png'),
    ('GCC RTT', plotters.plot_gcc_rtt, 1, 1, [
     'sender.stderr.feather'], 'gcc_rtt.png'),
    ('GCC Target Rates', plotters.plot_gcc_target_rates, 1, 1, [
        'sender.stderr.feather'], 'gcc_target_rates.png'),
    ('GCC Estimates', plotters.plot_gcc_estimates, 1, 1, [
     'sender.stderr.feather'], 'gcc_estimates.png'),
    ('GCC Usage and State', plotters.plot_gcc_usage_and_state, 1, 1,
     ['sender.stderr.feather'], 'gcc_usage_state.png'),
    ('SCTP Stats', plotters.plot_sctp_stats, 1, 1,
     ['sender.stderr.sctp.feather'], 'sctp_stats.png'),

    ('Encoding frame sizes', plotters.plot_encoding_frame_size, 1, 1, [
     'sender.stderr.feather'], 'encoding_frame_sizes.png'),
    ('Decoding frame sizes', plotters.plot_decoding_frame_size, 1, 1, [
        'receiver.stderr.feather'], 'receiver_frame_sizes.png'),

    ('Encoding time', plotters.plot_encoding_time, 1, 1, [
     'sender.stderr.feather'], 'encoding_time.png'),
    ('Decoding time', plotters.plot_decoding_time, 1, 1, [
     'receiver.stderr.feather'], 'decoding_time.png'),

    ('E2E Latency', plotters.plot_e2e_latency, 1, 1, [
     'sender.stderr.feather', 'receiver.stderr.feather'], 'e2e_latency.png'),
    ('Frame Latency e2e', plotters.plot_frame_latency, 1, 1, [
     'sender.stderr.feather', 'receiver.stderr.feather'], 'frame_latency.png'),
    ('Video Quality Metrics', plotters.plot_video_quality, 1, 1, [
     'video.quality.feather'], 'video_quality.png'),
    ('Encoded Video Rate', plotters.plot_video_rate, 1, 1, [
     'sender.stderr.feather'], 'video_rate.png'),
    ('Encoded Frame Sizes', plotters.plot_frame_size_dist, 1, 1, [
     'sender.stderr.feather'], 'video_frame_size_dist.png'),
    ('Encoded Frame Sizes', plotters.plot_frame_size, 1, 1, [
     'sender.stderr.feather'], 'video_frame_size.png'),


    # plots with several subfigs
    # pcap plots twice: plot first without dtls and override it if dtls present

    # Send rate + owd
    ('Send Rates + network owd', plotters.plot_all_send_rates_and_owd_pcaps_nodtls, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'config.feather'], 'all_send_rates_pcaps_owd.png'),
    ('Send Rates + network owd', plotters.plot_all_send_rates_and_owd_pcaps, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'ns4.dtls.feather', 'config.feather'], 'all_send_rates_pcaps_owd.png'),
    ('Send Rates + network owd', plotters.plot_rtp_rates_and_owd_quic, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather', 'receiver.feather'], 'quic_rates_owd.png'),
    ('Send Rates + network owd (quic overall)', plotters.plot_send_rates_and_owd_quic, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather', 'receiver.feather'], 'quic_rates_owd_overall.png'),

    # Send rate + loss
    ('Send Rates + losses', plotters.plot_all_send_rates_and_loss_pcaps_nodtls, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'config.feather'], 'all_send_rates_pcaps_loss.png'),
    ('Send Rates + losses', plotters.plot_all_send_rates_and_loss_pcaps, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'ns4.dtls.feather', 'config.feather'], 'all_send_rates_pcaps_loss.png'),
    ('Send Rates + losses', plotters.plot_rtp_rates_and_loss_quic, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather', 'receiver.feather'], 'quic_rates_loss.png'),
    ('Send Rates + losses (quic overall)', plotters.plot_send_rates_and_loss_quic, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather', 'receiver.feather'], 'quic_rates_loss_overall.png'),
]


async def parse_file(input, out_dir, ref_time=None):
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
        df = parsers.parse_pion_sctp_log(input, ref_time)
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
    if path.name in ['video.quality.csv', 'lost_frames.csv']:
        df = pd.read_csv(input)
        serializers.write_feather(
            df, Path(out_dir) / Path(Path(input).stem + '.feather'))


async def parse_config(input_dir):
    """parses config without saving it"""
    config_path = Path(input_dir) / Path('config.json')
    if not config_path.is_file():
        raise FileNotFoundError(f'config.json not found in {input_dir}')

    df = parsers.parse_json_log_no_convert(config_path)

    return df


async def parse_all_cmd(args):
    dir = Path(args.input)

    # Parse config.json to get timezone
    config = await parse_config(dir)
    ref = pd.Timestamp(config['time'][0])

    for file in dir.iterdir():
        if file.is_file():
            await parse_file(file, args.output, ref_time=ref)


async def parse_cmd(args):
    await parse_file(args.input, args.output)


async def plot_cmd(args):
    config_feather = Path(args.input) / Path('config.feather')
    config = serializers.read_feather(config_feather)
    start_time = pd.Timestamp(config['time'][0])

    for title, func, num_rows, num_column, files, out_name in plots:
        fig_height = 3*num_rows
        fig, ax = plt.subplots(
            nrows=num_rows, ncols=num_column, figsize=(8, fig_height), sharex=True)
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
            plt.close(fig)
            continue

        if num_column > 1 or num_rows > 1:
            fig.suptitle(title)
            axes = ax.flat if hasattr(ax, 'flat') else (
                ax if isinstance(ax, list) else [ax])
            for axis in axes:
                if axis.get_legend() is not None:
                    axis.legend(loc='upper right', fontsize='small')

        else:
            ax.set_title(title)
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.subplots_adjust(hspace=0.3)
        fig.savefig(Path(args.output) / Path(out_name), dpi=300)
        plt.close(fig)


async def generate_cmd(args):
    html_generator.generate_html(args.input)


async def plot_combis_cmd(args):
    if args.mode == 'version':
        plot_version_comparison.plot_version_comparison(
            args.input, args.output)
    elif args.mode == 'link':
        plot_version_comparison.plot_link_comparision(args.input, args.output)
    else:
        plot_version_comparison.plot_predefined_comparisons(
            args.input, args.output)


async def calc_video_metrics(args):
    video_quality.calculate_quality_metrics(
        args.reference, args.input, args.output)


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
    plot_combis.add_argument('-m', '--mode', choices=['version', 'link', "default"], default='version',
                             help='comparison mode: "version" combines by test version (e.g. each webrtc-gcc test), "link" combines by link type (e.g. each static test), "default" uses predefined combinations')
    plot_combis.set_defaults(func=plot_combis_cmd)

    video_qm = subparsers.add_parser(
        'video-quality', help='caluculate video quality metrics using ffmpeg')
    video_qm.add_argument(
        '-r', '--reference', help='reference video', required=True)
    video_qm.add_argument(
        '-i', '--input', help='folder that contains the test results, including the video with the name out.y4m', required=True)
    video_qm.add_argument(
        '-o', '--output', help='output directory for result csv\'s', required=True)
    video_qm.set_defaults(func=calc_video_metrics)

    args = parser.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
