import polars as pl
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from pathlib import Path
from collections import defaultdict

class Aggregator:
    def __init__(self, input_dir, output_dir, output_format='png'):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_format = output_format

    def aggregate(self):
        runs = [p for p in self.input_dir.iterdir() if p.is_dir()]
        common_experiments = []
        for run in runs:
            experiments = {run.name for run in run.iterdir() if run.is_dir()}
            common_experiments.append(experiments)
        common_experiments = set.intersection(*common_experiments)

        aggregated_results = defaultdict(lambda: defaultdict(dict))

        for experiment in common_experiments:
            scenario, application = experiment.split('_')[0], experiment.split('_')[1]
            bandwidth = int(scenario.split('-')[1].removesuffix('mbit'))
            delay = int(scenario.split('-')[2].removesuffix('ms'))
            print(f'aggregating {scenario}-{application}...')
            input_dirs = [str(run / experiment) for run in runs]
            delay_agg, rate_agg, file_transmission_duration = self.aggregate_experiment(input_dirs)
            aggregated_results[bandwidth][delay][application] = {
                'delay': delay_agg,
                'rate': rate_agg,
                'file_transmission_duration': file_transmission_duration if not file_transmission_duration.is_empty() else None
            }

        self.plot(aggregated_results)

    def aggregate_experiment(self, input_dirs):
        delay = []
        rate = []
        file_transmission_duration = []
        for input_dir in input_dirs:
            dfs = {
                p.stem: pl.read_parquet(p)
                for p in Path(input_dir).glob('*.parquet')
            }
            start_time = dfs['config']['time'][0]
            for k in dfs:
                if 'time' in dfs[k].columns:
                    dfs[k] = dfs[k].with_columns(
                        (pl.col('time') - start_time).alias('time_delta')
                        .cast(pl.Duration('us'))
                    )
            a = 'time'
            b = 'time_rx'
            delay_df = (
                dfs['rtp_packets']
                .select(pl.col(['time_delta', a, b]))
                .filter(
                    pl.col(a).is_not_null(),
                    pl.col(b).is_not_null(),
                ).with_columns(
                    (pl.col(b) - pl.col(a))
                    .dt.total_seconds(fractional=True).alias('latency')
                )
            )
            delay.append(delay_df)

            rtp_rx_rate = (
                dfs['rtp_packets'].filter(pl.col('time_rx').is_not_null())
                .group_by_dynamic('time', every='1s')
                .agg(
                    pl.col('payload-length').sum() * 8,
                    pl.col('time_delta').min()
                )
            )
            rate.append(rtp_rx_rate)

            if 'data_rx' in dfs and 'file' in input_dir:
                result = dfs['data_rx'].select([
                    pl.col("time").min().alias("start"),
                    pl.col("time").max().alias("end"),
                    (pl.col("time").max() - pl.col("time").min()).alias("duration"),
                ])
                file_transmission_duration.append(result)


        delay_agg = pl.concat(delay)
        rate_agg = pl.concat(rate)
        if len(file_transmission_duration) > 0:
            file_transmission_duration_agg = pl.concat(file_transmission_duration)
        else:
            file_transmission_duration_agg = pl.DataFrame()
        return delay_agg, rate_agg, file_transmission_duration_agg

    def plot(self, aggregated_results):
        width = 30
        fig, ax = plt.subplots(nrows=3, ncols=3, sharey='row', sharex='col', figsize=(width, width/2), layout='constrained')
        max_latencies = {}
        for i, bandwidth in enumerate(sorted(aggregated_results)):
            for j, delay in enumerate(sorted(aggregated_results[bandwidth])):
                name = f'{bandwidth}-{delay}'
                print(f'plotting {name}...')
                max_latency = 0
                for application in reversed(sorted(aggregated_results[bandwidth][delay])):
                    if 'file' in application:
                        duration = aggregated_results[bandwidth][delay][application]['file_transmission_duration']
                        print(f'{name}-{application}: file transmission duration: {duration["duration"].mean()} (min: {duration["duration"].min()}, max: {duration["duration"].max()})')
                        continue
                    delay_agg = aggregated_results[bandwidth][delay][application]['delay']
                    rate_agg = aggregated_results[bandwidth][delay][application]['rate']
                    delay_mean = delay_agg['latency'].mean()
                    delay_min = delay_agg['latency'].min()
                    delay_max = delay_agg['latency'].max()
                    max_latencies[delay] = max(max_latencies.get(delay, 0), delay_mean + delay_agg['latency'].std())
                    print(f'mean: {delay_mean}, std: {delay_agg['latency'].std()}, sum: {delay_mean + delay_agg['latency'].std()}, max: {max_latency}')
                    delay_err = [[delay_mean - delay_min], [delay_max - delay_mean]]
                    rate_mean = rate_agg['payload-length'].mean()
                    rate_min = rate_agg['payload-length'].min()
                    rate_max = rate_agg['payload-length'].max()
                    rate_err = [[rate_mean - rate_min], [rate_max - rate_mean]]
                    print(f'{application}: delay={delay_mean}, delay_q25={delay_min}, delay_q75={delay_max}, rate={rate_mean}, rate_q25={rate_min}, rate_q75={rate_max}')
                    print(f'{application}: delay_err={delay_err}, rate_err={rate_err}')
                    ax[i, j].errorbar(delay_mean, rate_mean, xerr=delay_agg['latency'].std(), yerr=rate_agg['payload-length'].std(),
                                      fmt='o', capsize=5, label=application.removesuffix('-gcc'))
                ax[i, j].set_title(f'{bandwidth} Mbit/s, {delay} ms')
                ax[i, j].set_xlim(left=0, right=1.1*max_latencies[delay])
                ax[i, j].set_ylim(bottom=0, top= 1.2*bandwidth*1e6)
                ax[i, j].yaxis.set_major_formatter(mticker.EngFormatter(unit='bit/s'))
                ax[i, j].xaxis.set_major_formatter(mticker.EngFormatter(unit='s'))
                ax[i, j].grid()
        fig.supxlabel('Delay')
        fig.supylabel('Rate')
        fig.legend(handles=ax[0, 0].get_legend_handles_labels()[0], labels=ax[0, 0].get_legend_handles_labels()[1],
                   loc='outside upper center', ncol=4)
        fig.savefig(self.output_dir / f'bw_delay_scatter.{self.output_format}')
        plt.close(fig)


def gr(x):
    return x * (5**.5 - 1) / 2