#!/usr/bin/env python

import argparse

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import parquetizer
import plotter as plotter
import html_generator
import video_quality
import aggregate

import matplotlib

matplotlib.rcParams.update({'font.size': 20})
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

def parquetize(input, output):
    try:
        parquetizer.Parquetizer(input, output).parquetize()
    except Exception as e:
        print(f'failed to parquetize: {input} -> {output}: {e}')


def parquetize_cmd(args):
    if not args.sequential:
        run_parallel(parquetize, args.input, args.output, filter=args.filter)
    else:
        inputs = Path(args.input).iterdir()
        if len(args.filter) > 0:
            inputs = [input for input in inputs if args.filter in input.name]
        for input in inputs:
            output = Path(args.output) / Path(args.input).name / input.name
            parquetize(input, output)


def plot(input, output, format='png'):
    try:
        plotter.Plotter(input, output, output_format=format).plot()
    except Exception as e:
        print(f'failed to plot: {input} -> {output}: {e}')


def plot_cmd(args):
    if not args.sequential:
        run_parallel(plot, args.input, args.output, filter=args.filter, common_args=args.format)
    else:
        for input in Path(args.input).iterdir():
            if len(args.filter) == 0 or args.filter in input.name:
                output = Path(args.output) / Path(args.input).name / input.name
                plot(input, output, format=args.format)


def aggregate_cmd(args):
    try:
        aggregate.Aggregator(args.input, args.output, args.format).aggregate()
    except Exception as e:
        print(f'failed to aggregate: {args.input} -> {args.output}: {e}')


def run_parallel(func, input, output, common_args=None, filter=''):
    inputs = []
    outputs = []
    args = []
    for subdir_in in Path(input).iterdir():
        if subdir_in.is_dir() and (len(filter) == 0 or filter in subdir_in.name):
            inputs.append(subdir_in)
            subdir_out = Path(output) / Path(input).name / subdir_in.name
            outputs.append(subdir_out)
            if common_args is not None:
                args.append(common_args)
    with ProcessPoolExecutor() as executor:
        if len(args) > 0:
            list(executor.map(func, inputs, outputs, args))
        else:
            list(executor.map(func, inputs, outputs))


def generate_cmd(args):
    html_generator.generate_html(args.input)


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
    parquetize.add_argument('--filter', default='')
    parquetize.set_defaults(func=parquetize_cmd)

    plot = subparsers.add_parser('plot', help='reads a data frame from a '
                                 'feather file and creates plots')
    plot.add_argument(
        '-i', '--input', help='input directory', required=True)
    plot.add_argument(
        '-o', '--output', help='output directory', required=True)
    plot.add_argument('-f', '--format', default='png',
                      help='output file format, e.g., \'pdf\', or \'png\'')
    plot.add_argument('--filter', default='')
    plot.set_defaults(func=plot_cmd)

    aggregate = subparsers.add_parser(
        'aggregate', help='aggregates results from several experiments and creates comparison plots')
    aggregate.add_argument(
        '-i', '--input', help='input directory', required=True)
    aggregate.add_argument(
        '-o', '--output', help='output directory', required=True)
    # aggregate.add_argument('--mode', default='version',
    #                        help='comparison mode, e.g., \'version\' to compare different versions, or \'link\' to compare different link configurations')
    aggregate.add_argument('-f', '--format', default='png',
                           help='output file format, e.g., \'pdf\', or \'png\'')
    aggregate.set_defaults(func=aggregate_cmd)

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
