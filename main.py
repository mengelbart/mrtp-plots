#!/usr/bin/env python

import argparse

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import parquetizer
import plotters
import plot2
import html_generator
import plot_version_comparison
import video_quality

import matplotlib

matplotlib.rcParams.update({'font.size': 20})

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
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather', 'sender.roq.feather'], 'all_send_rates_qlog.png'),
    ('Receive Rates (qlog)', plotters.plot_all_recv_rates_qlog, 1, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'receiver.feather', 'sender.roq.feather'], 'all_recv_rates_qlog.png'),

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
    ('Network OWD (QUIC qlog)', plotters.plot_qlog_owd, 1, 1, ['sender.feather',
     'receiver.feather'], 'quic_owd.png'),
    ('RTP OWD', plotters.plot_rtp_owd_log_udp, 1, 1, ['sender.stderr.feather',
     'receiver.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'config.feather'], 'rtp_owd_log_stacked.png'),
    ('RTP OWD', plotters.plot_rtp_owd_log_roq, 1, 1, ['sender.stderr.feather',
     'receiver.stderr.feather', 'sender.feather'], 'rtp_owd_quic_stacked.png'),
    ('RTP OWD', plotters.plot_rtp_owd_log_udp_overall, 1, 1, ['sender.stderr.feather',
     'receiver.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'config.feather'], 'rtp_owd_log.png'),
    ('RTP OWD', plotters.plot_rtp_owd_log_roq_overall, 1, 1, ['sender.stderr.feather',
     'receiver.stderr.feather', 'sender.feather'], 'rtp_owd_quic.png'),

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
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather', 'receiver.feather', 'sender.roq.feather'], 'quic_rates_owd.png'),
    ('Send Rates + network owd (quic overall)', plotters.plot_send_rates_and_owd_quic, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather', 'receiver.feather'], 'quic_rates_owd_overall.png'),

    # Send rate + loss
    ('Send Rates + losses', plotters.plot_all_send_rates_and_loss_pcaps_nodtls, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'config.feather'], 'all_send_rates_pcaps_loss.png'),
    ('Send Rates + losses', plotters.plot_all_send_rates_and_loss_pcaps, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'ns4.rtp.feather', 'ns1.rtp.feather', 'ns4.dtls.feather', 'config.feather'], 'all_send_rates_pcaps_loss.png'),
    ('Send Rates + losses', plotters.plot_rtp_rates_and_loss_quic, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather', 'receiver.feather', 'sender.roq.feather'], 'quic_rates_loss.png'),
    ('Send Rates + losses (quic overall)', plotters.plot_send_rates_and_loss_quic, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather', 'sender.feather', 'receiver.feather'], 'quic_rates_loss_overall.png'),

    # plots for understanding the encoder behavior
    ('frame size + tr', plotters.plot_frame_size_and_tr, 2, 1, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather'], 'video_frame_tr.png'),

    # other
   ('completion time', plotters.plot_file_completion, 1, 1, [
    'sender.stderr.feather', 'receiver.stderr.feather'], 'comp_time.png'),
]


def parquetize(input, output):
    try:
        parquetizer.Parquetizer(input, output).parquetize()
    except Exception as e:
        print(f'failed to parquetize: {input} -> {output}: {e}')


def parquetize_cmd(args):
    if not args.sequential:
        run_parallel(parquetize, args.input, args.output)
    else:
        for input in Path(args.input).iterdir():
            output = Path(args.output) / Path(args.input).name / input.name
            parquetize(input, output)


def plot(input, output):
    try:
        plot2.Plotter(input, output).plot()
    except Exception as e:
        print(f'failed to plot: {input} -> {output}: {e}')


def plot_cmd(args):
    if not args.sequential:
        run_parallel(plot, args.input, args.output)
    else:
        for input in Path(args.input).iterdir():
            output = Path(args.output) / Path(args.input).name / input.name
            plot(input, output)


def run_parallel(func, input, output):
    inputs = []
    outputs = []
    for subdir_in in Path(input).iterdir():
        if subdir_in.is_dir():
            inputs.append(subdir_in)
            subdir_out = Path(output) / Path(input).name / subdir_in.name
            outputs.append(subdir_out)

    with ProcessPoolExecutor() as executor:
        list(executor.map(func, inputs, outputs))


def generate_cmd(args):
    html_generator.generate_html(args.input)


async def plot_combis_cmd(args):
    if args.mode == 'version':
        plot_version_comparison.plot_version_comparison(
            args.input, args.output)
    elif args.mode == 'link':
        plot_version_comparison.plot_link_comparision(args.input, args.output)
    elif args.mode == 'avgs':
        plot_version_comparison.calc_avgs_comparision(args.input, args.output)
    else:
        plot_version_comparison.plot_predefined_comparisons(
            args.input, args.output)


def calc_video_metrics(args):
    video_quality.calculate_quality_metrics(
        args.reference, args.input, args.output)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(help='sub-command help', required=True)
    parser.add_argument('-s', '--sequential',
                        action=argparse.BooleanOptionalAction, default=False,
                        help='run sequential instead of parallel')

    parquetize = subparsers.add_parser('parquetize', help='alternative to '
                                       'parse parses data and stores it in a '
                                       'single parquet file per experiment')
    parquetize.add_argument(
        '-i', '--input', help='input directory', required=True)
    parquetize.add_argument(
        '-o', '--output', help='output directory', required=True)
    parquetize.set_defaults(func=parquetize_cmd)

    plot = subparsers.add_parser('plot', help='reads a data frame from a '
                                 'feather file and creates plots')
    plot.add_argument(
        '-i', '--input', help='input directory', required=True)
    plot.add_argument(
        '-o', '--output', help='output directory', required=True)
    plot.add_argument('-f', '--format', default='png',
                      help='output file format, e.g., \'pdf\', or \'png\'')
    plot.set_defaults(func=plot_cmd)

    generate = subparsers.add_parser(
        'generate', help='generates a HTML site to show results')

    generate.set_defaults(func=generate_cmd)
    generate.add_argument(
        '-i', '--input', help='input directory', required=True)

    video_qm = subparsers.add_parser(
        'video-quality', help='caluculate video quality metrics using ffmpeg')
    video_qm.add_argument(
        '-r', '--reference', help='reference video', required=True)
    video_qm.add_argument('-i', '--input', required=True,
                          help='folder that contains the test results, '
                               'including the video with the name out.y4m')
    video_qm.add_argument('-o', '--output', required=True,
                          help='output directory for result csv\'s')
    video_qm.set_defaults(func=calc_video_metrics)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
