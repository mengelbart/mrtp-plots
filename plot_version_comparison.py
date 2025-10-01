from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import plotters
import serializers

# Settings for the plots
FIG_SIZE = (8, 3)
FIG_DPI = 300


def _save_rate_graph(ax, fig, image_path, legend, yname):
    ax.yaxis.set_major_formatter(mticker.EngFormatter(unit="bit/s"))
    _save_delay_graph(ax, fig, image_path, legend, yname)


def _save_delay_graph(ax, fig, image_path, legend, yname):
    ax.legend(legend)
    ax.set_ylabel(yname)
    ax.set_xlabel("Time")
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f"{x:.0f}s"))
    ax.set_title(image_path.name.split("/")[-1].replace(".png", ""))

    fig.tight_layout()
    fig.autofmt_xdate()
    fig.savefig(image_path, bbox_inches="tight")
    plt.close()


def _get_start_time(path_to_case):
    config_feather = Path(path_to_case) / Path("config.feather")
    config = serializers.read_feather(config_feather)
    return pd.Timestamp(config["time"][0])


def _plot_capacity(ax, legend, path_to_case):
    start_time = _get_start_time(path_to_case)
    cap_feather = Path(path_to_case) / Path("tc.feather")
    cap = serializers.read_feather(cap_feather)
    plotters.plot_capacity(ax, start_time, cap)
    legend.append("capacity")


def plot_delay(testtype, cases, out):
    """Creates combined delay plot"""
    legend = []
    image_name = Path(out) / Path(f"{testtype}_delay.png")

    fig, ax = plt.subplots(dpi=FIG_DPI, figsize=FIG_SIZE)

    for case in cases:
        start_time = _get_start_time(case[1])

        feather_sender = Path(case[1]) / Path("sender.stderr.feather")
        feather_recv = Path(case[1]) / Path("receiver.stderr.feather")

        if feather_sender.is_file() and feather_recv.is_file():
            df_recv = serializers.read_feather(feather_recv)
            df_sender = serializers.read_feather(feather_sender)

            plotted = plotters.plot_rtp_owd_log(
                ax, start_time, df_sender, df_recv)

            if plotted:
                legend.append(case[2] + " rtp")
                continue  # TODO: plot both?

        qlog_tx_feather = Path(case[1]) / Path("sender.feather")
        qlog_rx_feater = Path(case[1]) / Path("receiver.feather")

        if qlog_tx_feather.is_file() and qlog_rx_feater.is_file():
            qlog_tx_df = serializers.read_feather(qlog_tx_feather)
            qlog_rx_df = serializers.read_feather(qlog_rx_feater)

            plotted = plotters.plot_qloq_owd(
                ax, start_time, qlog_tx_df, qlog_rx_df
            )

            if plotted:
                legend.append(case[2] + " qlog")

    _save_delay_graph(ax, fig, image_name, legend, "Latency")


def plot_send_rate(testtype, cases, out):
    legend = []
    image_name = Path(out) / Path(f"{testtype}_send-rate.png")

    fig, ax = plt.subplots(dpi=FIG_DPI, figsize=FIG_SIZE)

    if len(cases) != 0:
        _plot_capacity(ax, legend, cases[0][1])

    # graphs
    for case in cases:
        start_time = _get_start_time(case[1])

        feather_file = Path(case[1]) / Path("sender.stderr.feather")
        if feather_file.is_file():
            df = serializers.read_feather(feather_file)
            plotted = plotters.plot_rtp_rate(ax, start_time, df, case[2])
            if plotted:
                legend.append(case[2] + " rtp")

            plotted = plotters.plot_data_rate(ax, start_time, df, case[2])
            if plotted:
                legend.append(case[2] + " qlog")

    _save_rate_graph(ax, fig, image_name, legend, "Rate")


def plot_target_rate(testtype, cases, out):
    legend = []
    image_name = Path(out) / Path(f"{testtype}_target-rate.png")

    fig, ax = plt.subplots(dpi=FIG_DPI, figsize=FIG_SIZE)

    if len(cases) != 0:
        _plot_capacity(ax, legend, cases[0][1])

    # graphs
    for case in cases:
        start_time = _get_start_time(case[1])

        feather_file = f"{case[1]}/sender.stderr.feather"
        df = serializers.read_feather(feather_file)
        plotters.plot_target_rate(
            ax, start_time, df, event_name="NEW_TARGET_RATE")

        legend.append(case[2])

    _save_rate_graph(ax, fig, image_name, legend, "Rate")


def plot_everything(testtype, cases, out):
    """Plots all version comparison plots"""
    plot_delay(testtype, cases, out)
    plot_send_rate(testtype, cases, out)
    plot_target_rate(testtype, cases, out)


def get_test_types(testcases):
    """Get all unique test types in testcase tuple list. Types: static_webrtc-nada, ..."""
    testtypes = [case[0] for case in testcases]
    return set(testtypes)


def plot_it(testcases, out):
    sorted_testcases = sorted(testcases, key=lambda tup: tup[2])

    testtypes = get_test_types(sorted_testcases)

    # plot for each test type
    for testtype in testtypes:
        cases = list(
            filter(lambda case: case[0] == testtype, sorted_testcases))
        plot_everything(testtype, cases, out)


def get_all_iter_testcases(dir: str):
    """Returns list of tupels (testtype, path, name) of each test case"""
    testcases = []

    tests_dir = Path(dir)
    for test_iteration in tests_dir.iterdir():
        if test_iteration.is_dir():
            for test_case in test_iteration.iterdir():
                if test_case.is_dir():
                    testcase = (test_case.name, str(
                        test_case), test_iteration.name)
                    testcases.append(testcase)

    return testcases


def plot_version_comparison(input: str, output: str):
    plot_it(get_all_iter_testcases(input), output)
