from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
import plotters
import serializers

# Settings for the plots
FIG_SIZE = (8, 3)
FIG_DPI = 300

predefined_plots = [
    # (name-of-plot, [(testcase, case-name), ...])
    ("defaults", [("static_quic-rtp-nada-pacing", "roq"),
     ("static_webrtc-rtp-nada-pacing", "webrtc")]),
]


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


def plot_fdelay(name, cases, out):
    """Creates combined frame delay plot"""
    legend = []
    image_name = Path(out) / Path(f"{name}_fdelay.png")

    fig, ax = plt.subplots(dpi=FIG_DPI, figsize=FIG_SIZE)

    for case in cases:
        start_time = _get_start_time(case[1])

        feather_sender = Path(case[1]) / Path("sender.stderr.feather")
        feather_recv = Path(case[1]) / Path("receiver.stderr.feather")

        if feather_sender.is_file() and feather_recv.is_file():
            df_recv = serializers.read_feather(feather_recv)
            df_sender = serializers.read_feather(feather_sender)

            plotted = plotters.plot_frame_latency(
                ax, start_time, df_sender, df_recv)

            if plotted:
                legend.append(case[2])

    _save_delay_graph(ax, fig, image_name, legend, "Latency")


def plot_owd_cdf(name, cases, out):
    legend = []
    image_name = Path(out) / Path(f"{name}_delay_cdf.png")

    fig, ax = plt.subplots(dpi=FIG_DPI, figsize=FIG_SIZE)

    for case in cases:
        start_time = _get_start_time(case[1])

        # pcap owd
        rtp_pcap_tx = Path(case[1]) / Path('ns4.rtp.feather')
        rtp_pcap_rx = Path(case[1]) / Path('ns1.rtp.feather')

        if rtp_pcap_tx.is_file() and rtp_pcap_rx.is_file():
            rtp_pcap_tx_df = serializers.read_feather(rtp_pcap_tx)
            rtp_pcap_rx_df = serializers.read_feather(rtp_pcap_rx)

            plotted = plotters.plot_rtp_owd_pcap_cdf(
                ax, start_time, rtp_pcap_tx_df, rtp_pcap_rx_df)
            if plotted:
                legend.append(case[2])
            continue

        # quic owd TODO: also only rtp owd?
        qlog_tx_feather = Path(case[1]) / Path("sender.feather")
        qlog_rx_feater = Path(case[1]) / Path("receiver.feather")

        if qlog_tx_feather.is_file() and qlog_rx_feater.is_file():
            qlog_tx_df = serializers.read_feather(qlog_tx_feather)
            qlog_rx_df = serializers.read_feather(qlog_rx_feater)

            plotted = plotters.plot_qlog_owd_cdf(
                ax, start_time, qlog_tx_df, qlog_rx_df
            )

            if plotted:
                legend.append(case[2])

    ax.legend(legend)
    ax.set_ylabel("CDF")
    ax.set_xlabel("latency (ms)")
    ax.set_title(image_name.name.split("/")[-1].replace(".png", ""))
    ax.xaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, pos: f'{x*1000:.0f}'))

    fig.tight_layout()
    fig.savefig(image_name, bbox_inches="tight")
    plt.close()


def plot_target_rate(name, cases, out):
    legend = []
    image_name = Path(out) / Path(f"{name}_target-rate.png")

    fig, ax = plt.subplots(dpi=FIG_DPI, figsize=FIG_SIZE)

    if len(cases) != 0:
        _plot_capacity(ax, legend, cases[0][1])

    # graphs
    for case in cases:
        start_time = _get_start_time(case[1])

        feather_file = f"{case[1]}/sender.stderr.feather"
        df = serializers.read_feather(feather_file)
        plotters.plot_target_rate(
            ax, start_time, df, event_name="NEW_TARGET_MEDIA_RATE")

        legend.append(case[2])

    _save_rate_graph(ax, fig, image_name, legend, "Rate")


def plot_video_quality(plot_name, cases, out, name, plot_fct): # TODO: remove additonal name arg
    legend = []
    image_name = Path(out) / Path(f"{plot_name}_{name}.png")

    fig, ax = plt.subplots(dpi=FIG_DPI, figsize=FIG_SIZE)

    # graphs
    for case in cases:
        feather_file = Path(case[1]) / Path("video.quality.feather")
        if not feather_file.is_file():
            continue

        qm_df = serializers.read_feather(feather_file)
        plot_fct(ax, None, qm_df)
        legend.append(case[2])

    ax.legend(legend)
    ax.set_ylabel("CDF")
    ax.set_xlabel(name)
    ax.set_title(image_name.name.split("/")[-1].replace(".png", ""))

    fig.tight_layout()
    fig.savefig(image_name, bbox_inches="tight")
    plt.close()


def plot_everything(name, cases, out):
    """Plots all version comparison plots"""
    plot_fdelay(name, cases, out)
    plot_owd_cdf(name, cases, out)
    plot_target_rate(name, cases, out)
    plot_video_quality(name, cases, out, "ssim",
                       plotters.plot_video_quality_ssim_cdf)
    plot_video_quality(name, cases, out, "psnr",
                       plotters.plot_video_quality_psnr_cdf)


def get_test_types(testcases):
    """Get all unique test types in testcase tuple list. Types: static_webrtc-nada, ..."""
    testtypes = [case[0] for case in testcases]
    return set(testtypes)


def _get_link_types(testcases):
    """Get all unique link types in testcase tuple list. Types: static, ..."""
    linktypes = [case[0].split("_")[0] for case in testcases]
    return set(linktypes)


def plot_by_testtype(testcases, out):
    sorted_testcases = sorted(testcases, key=lambda tup: tup[2])

    testtypes = get_test_types(sorted_testcases)

    # plot for each test type
    for testtype in testtypes:
        cases = list(
            filter(lambda case: case[0] == testtype, sorted_testcases))
        plot_everything(testtype, cases, out)


def plot_by_link(testcases, out):
    sorted_testcases = sorted(testcases, key=lambda tup: tup[2])
    sorted_testcases = [(tup[0], tup[1], tup[0])
                        for tup in sorted_testcases]  # change name

    link_types = _get_link_types(sorted_testcases)

    # plot for each test type
    for link_type in link_types:
        cases = list(
            filter(lambda case: case[0].split("_")[0] == link_type, sorted_testcases))
        plot_everything(link_type, cases, out)


def plot_by_predefined(testcases, out):
    sorted_iterations = sorted(testcases, key=lambda tup: tup[2])

    for plot in predefined_plots:
        plot_name = plot[0]
        cases = []
        for case in plot[1]:
            for test_iter in sorted_iterations:
                if test_iter[0] == case[0]:
                    print(f"match: ({case[0]}, {case[1]}) -> {test_iter[1]}")
                    cases.append((test_iter[0], test_iter[1], case[1]))
        plot_everything(plot_name, cases, out)


def get_all_testcases(dir: str):
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


def plot_link_comparision(input: str, output: str):
    """Combine results per link type. E.g. each static test combined"""
    plot_by_link(get_all_testcases(input), output)


def plot_version_comparison(input: str, output: str):
    """Combine results per version. E.g. each iteration of webrtc-gcc-pacing"""
    plot_by_testtype(get_all_testcases(input), output)


def plot_predefined_comparisons(input: str, output: str):
    """Plot the statically defined comparisons in predefined_plots"""
    plot_by_predefined(get_all_testcases(input), output)
