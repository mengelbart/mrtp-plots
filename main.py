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

plots = [
    ('RTP Rates', plotters.plot_rtp_rates, [
     'tc.feather', 'sender.stderr.feather', 'receiver.stderr.feather'], 'rtp_rates.png'),
    # ('RTP Send Rate', plotters.plot_rtp_rate, [
    #  'sender.stderr.feather'], 'rtp_send_rate.png'),
    # ('RTP Recv Rate', plotters.plot_rtp_rate, [
    #  'receiver.stderr.feather'], 'rtp_recv_rate.png'),
    ('RTP Loss Rate', plotters.plot_rtp_loss_pcap, ['ns4.rtp.feather',
     'ns1.rtp.feather'], 'rtp_loss.png'),
    ('RTP Loss Rate', plotters.plot_rtp_loss_log, ['sender.stderr.feather',
     'receiver.stderr.feather'], 'rtp_loss_log.png'),
    ('RTP OWD', plotters.plot_rtp_owd_pcap, ['ns4.rtp.feather',
     'ns1.rtp.feather'], 'rtp_owd.png'),
    ('RTP OWD', plotters.plot_rtp_owd_log, ['sender.stderr.feather',
     'receiver.stderr.feather'], 'rtp_owd_log.png'),
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
]


async def parse_file(input, out_dir):
    path = Path(input)
    if path.name in ['config.json', 'tc.log', 'receiver.stderr.log', 'sender.stderr.log']:
        df = parsers.parse_json_log(input)
        serializers.write_feather(
            df, Path(out_dir) / Path(input).with_suffix('.feather').name)
    if path.suffix == '.pcap':
        rtp, rtcp = await parsers.parse_pcap(input)
        if rtp.empty or rtcp.empty:
            return

        serializers.write_feather(
            rtp, Path(out_dir) / Path(Path(input).stem + '.rtp.feather'))
        serializers.write_feather(rtcp, Path(
            out_dir) / Path(Path(input).stem + '.rtcp.feather'))


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
    args = parser.parse_args()
    asyncio.run(args.func(args))


if __name__ == "__main__":
    main()
