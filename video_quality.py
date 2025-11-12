from pathlib import Path
import subprocess
import cv2
import ffmpeg_quality_metrics as ffmpeg
import shutil

import parsers


def get_lost_frames(sender_log_file, receiver_log_file):
    tx_df = parsers.parse_json_log(sender_log_file)
    rx_df = parsers.parse_json_log(receiver_log_file)

    tx_before_enc = tx_df[tx_df['msg'] == 'encoder sink'].copy()

    tx_mapping = tx_df[tx_df['msg'] == 'rtp to pts mapping'].copy()
    rx_mapping = rx_df[rx_df['msg'] == 'rtp to pts mapping'].copy()

    tx_frames = tx_df[tx_df['msg'] == 'encoder src'].copy()
    rx_frames = rx_df[rx_df['msg'] == 'decoder src']

    # encoder starts at different timepoint => shift pts to 0
    tx_frame_min_pts = tx_frames['pts'].min()
    tx_frames['pts-shifted'] = tx_frames['pts'] - tx_frame_min_pts

    tx_before_enc['pts-shifted'] = tx_before_enc['pts']  # so we can  join

    # join pts from before encoder with log after encoder
    # => see if encoder dropped frames
    tx_all = tx_before_enc.merge(
        tx_frames, on='pts-shifted', how='left', suffixes=('_ori', ''))

    # left join all frames with mapping => also keep frames that do not have a mapping
    tx_merged = tx_all.merge(
        tx_mapping, on='pts', how='left', suffixes=('_frame', '_mapping'))

    # join mapping and frames on receiver side
    rx_merged = rx_mapping.merge(
        rx_frames, on='pts', how='inner', suffixes=('_mapping', '_frame'))

    # Left anti join: get rows from tx_merged where rtp-timestamp is NOT in rx_merged
    # <=> all frames that where not played out
    lost_frames_df = tx_merged[~tx_merged['rtp-timestamp_mapping']
                               .isin(rx_merged['rtp-timestamp_mapping'])]

    # Remove lost frames with timestamp larger than max received timestamp
    max_rx_timestamp = rx_merged['rtp-timestamp_mapping'].max()
    lost_frames_df = lost_frames_df[lost_frames_df['rtp-timestamp_mapping']
                                    <= max_rx_timestamp]

    for _, row in lost_frames_df.iterrows():
        print(
            f"Lost frame: frame Nr: {row['frame-count_ori']}, Timestamp: {row['rtp-timestamp_mapping']}")

    return lost_frames_df


def remove_frames(ref_file: Path, lost_frames, out_file: Path, num_frames: int):
    frames_to_skip = sorted(lost_frames['frame-count_ori'].astype(int))

    with open(ref_file, 'rb') as infile, open(out_file, 'wb') as outfile:
        header = infile.readline()

        if not header.startswith(b'YUV4MPEG2'):
            raise ValueError("Not a valid Y4M file")

        outfile.write(header)

        # Parse dimensions
        header_str = header.decode()
        width = int(header_str.split('W')[1].split()[0])
        height = int(header_str.split('H')[1].split()[0])

        # TODO: what about other formats
        # YUV420 frame size
        frame_data_size = width * height + 2 * ((width // 2) * (height // 2))

        frame_idx = 0
        frames_written = 0

        for _ in range(num_frames):
            # Read "FRAME\n" text marker
            frame_marker = infile.readline()
            if not frame_marker:
                break

            # Read frame exactly
            frame_data = infile.read(frame_data_size)
            if len(frame_data) != frame_data_size:
                break

            # Write if not skipped
            if frame_idx not in frames_to_skip:
                outfile.write(frame_marker)
                outfile.write(frame_data)
                frames_written += 1

            frame_idx += 1

            if frame_idx % 100 == 0:
                print(f"Processed {frame_idx}/{num_frames} frames", end='\r')

        print(f"\nWrote {frames_written} frames")


def calculate_quality_metrics(ref_file, dist_file, out_dir):
    cam = cv2.VideoCapture(ref_file)
    fps = cam.get(cv2.CAP_PROP_FPS)

    lost_frames = get_lost_frames(f'{out_dir}/sender.stderr.log',
                                  f'{out_dir}/receiver.stderr.log')

    num_frames = 3100  # TODO: get from config

    if len(lost_frames) > 0:
        tmp_file = Path(f"{out_dir}/tmp_ref_file.y4m")
        remove_frames(Path(ref_file), lost_frames, tmp_file, num_frames)

        qm = ffmpeg.FfmpegQualityMetrics(
            ref=str(tmp_file), dist=dist_file, framerate=fps, progress=True)
        qm.calculate()

        tmp_file.unlink()

    else:
        qm = ffmpeg.FfmpegQualityMetrics(
            ref=ref_file, dist=dist_file, framerate=fps, progress=True, num_frames=num_frames)

        qm.calculate()

    csv_output = qm.get_results_csv()

    with open(f"{out_dir}/video.quality.csv", "w+") as f:
        f.write(csv_output)
