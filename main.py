#!/usr/bin/env python

import argparse
import json
import glob
import os

import pandas as pd
import pyarrow as pa
import pyarrow.feather as feather
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import matplotlib.pyplot as plt
import jinja2


def parse_json_log(log_file, out_file):
    with open(log_file, 'r') as f:
        data = []
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    df = pd.json_normalize(data)
    table = pa.Table.from_pandas(df)
    feather.write_feather(table, out_file)


def read_feather(file):
    table = pa.ipc.open_file(file).read_all()
    df = table.to_pandas()
    return df


def plot_rtp_rate(df, out_file):
    df = df[df['msg'] == 'rtp packet'].copy()
    df['rate'] = df['rtp-packet.payload-length'] * 80
    df['timestamp'] = pd.to_datetime(df['time'])
    df.set_index('timestamp', inplace=True)
    df = df.resample('100ms').sum().copy()
    starttime = df.index[0]
    df['second'] = (df.index - starttime).total_seconds()
    df.set_index('second', inplace=True)

    fig, ax = plt.subplots(nrows=1, ncols=1)
    ax.plot(df.index, df['rate'], linewidth=0.5)
    ax.set_ylim(bottom=0, top=3e6)
    ax.set_title('RTP Rate')
    ax.set_xlabel('Time')
    ax.set_ylabel('Rate')
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x:.0f}s'))
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit='bit/s'))
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(out_file, dpi=300)
    plt.close(fig)


def plot_rtp_rate1(df, out_file):
    fig, ax = plt.subplots(nrows=1, ncols=1)  # create figure & 1 axis
    ax.plot([0, 1, 2], [10, 20, 3])
    fig.savefig(out_file)   # save the figure to file
    fig.tight_layout()
    plt.close(fig)


def generate_html(input):
    image_paths = glob.glob(f'{input}/*/rtp_rate.png', recursive=True)
    images = []
    for path in image_paths:
        html_path = path.replace(os.sep, '/')
        dir_name = os.path.basename(os.path.dirname(path))
        images.append({'path': html_path, 'caption': dir_name})

    env = jinja2.Environment(loader=jinja2.FileSystemLoader("templates"))
    template = env.get_template("index.html")

    rendered_html = template.render(images=images)

    with open('index.html', 'w') as f:
        f.write(rendered_html)


def parse_cmd(args):
    parse_json_log(args.input, args.output)


def plot_cmd(args):
    df = read_feather(args.input)
    plot_rtp_rate(df, args.output)


def generate_cmd(args):
    generate_html(args.input)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    subparsers = parser.add_subparsers(help='sub-command help', required=True)

    parse = subparsers.add_parser(
        'parse', help='parses log files, converts them to a data frame and serializes the data frame as feather file')

    parse.add_argument('-i', '--input', help='input log file', required=True)
    parse.add_argument(
        '-o', '--output', help='output feather file', required=True)
    parse.set_defaults(func=parse_cmd)

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
    args.func(args)


if __name__ == "__main__":
    main()
