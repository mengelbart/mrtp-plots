#!/usr/bin/env python

import argparse
import asyncio

from pathlib import Path

import matplotlib.pyplot as plt

import parsers
import plotters
import html_generator
import serializers


async def parse_file(input, out_dir):
    path = Path(input)
    if path.name in ['receiver.stderr.log', 'sender.stderr.log']:
        df = parsers.parse_json_log(input)
        serializers.write_feather(
            df, Path(out_dir) / Path(input).with_suffix('.feather').name)
    if path.suffix == '.pcap':
        rtp, rtcp = await parsers.parse_pcap(input)
        serializers.write_feather(
            rtp, Path(out_dir) / Path(input).with_suffix('.pcap.feather').name)
        serializers.write_feather(rtcp, Path(
            out_dir) / Path(input).with_suffix('.pcap.feather').name)


async def parse_all_cmd(args):
    dir = Path(args.input)
    for file in dir.iterdir():
        if file.is_file():
            await parse_file(file, args.output)


async def parse_cmd(args):
    await parse_file(args.input, args.output)


async def plot_cmd(args):
    df = serializers.read_feather(args.input)
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=(8, 3))
    plotters.plot_rtp_rate(ax, df)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(args.output, dpi=300)
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
        '-i', '--input', help='input feather file', required=True)
    plot.add_argument(
        '-o', '--output', help='output plot file', required=True)
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
