import ffmpeg_quality_metrics as ffmpeg


def calculate_quality_metrics(ref_file, dist_file, out_dir):
    qm = ffmpeg.FfmpegQualityMetrics(
        ref=ref_file, dist=dist_file, framerate=30.0, progress=True)

    qm.calculate()

    csv_output = qm.get_results_csv()

    with open(f"{out_dir}/video.quality.csv", "w+") as f:
        f.write(csv_output)
