from pathlib import Path

import cv2
import ffmpeg_quality_metrics as ffmpeg
from pandas import DataFrame

import parsers


def map_frames_sender_pipeline(tx_df):
    tx_before_enc = tx_df[tx_df['msg'] == 'encoder sink'].copy()

    tx_mapping = tx_df[tx_df['msg'] == 'rtp to pts mapping'].copy()
    tx_frames = tx_df[tx_df['msg'] == 'encoder src'].copy()

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

    return tx_merged


def map_frames_receiver_pipeline(rx_df):
    rx_mapping = rx_df[rx_df['msg'] == 'rtp to pts mapping'].copy()
    rx_frames = rx_df[rx_df['msg'] == 'decoder src']

    # join mapping and frames on receiver side
    rx_merged = rx_mapping.merge(
        rx_frames, on='pts', how='inner', suffixes=('_mapping', '_frame'))

    return rx_merged


def get_lost_frames(sender_log_file, receiver_log_file):
    tx_df = parsers.parse_json_log(sender_log_file)
    tx_merged = map_frames_sender_pipeline(tx_df)

    rx_df = parsers.parse_json_log(receiver_log_file)
    rx_merged = map_frames_receiver_pipeline(rx_df)

    # Left anti join: get rows from tx_merged where rtp-timestamp is NOT in rx_merged
    # <=> all frames that where not played out
    lost_frames_df = tx_merged[~tx_merged['rtp-timestamp_mapping']
                               .isin(rx_merged['rtp-timestamp_mapping'])]

    # Remove lost frames with timestamp larger than max received timestamp
    max_rx_timestamp = rx_merged['rtp-timestamp_mapping'].max()
    lost_frames_df = lost_frames_df[lost_frames_df['rtp-timestamp_mapping']
                                    <= max_rx_timestamp]

    return lost_frames_df


def export_lost_frames_csv(lost_frames: DataFrame, out_file: Path):
    export_df = lost_frames[['frame-count_ori', 'rtp-timestamp_mapping']]
    export_df = export_df.rename(columns={
        'frame-count_ori': 'frame_number',
        'rtp-timestamp_mapping': 'rtp_timestamp'
    })
    export_df.to_csv(out_file, index=False)


def remove_frames(ref_file: Path, lost_frames, out_file: Path, num_frames: int):
    frames_to_skip = sorted(lost_frames['frame-count_ori'].astype(int))

    # Example:
    # YUV4MPEG2 <tagged-fields>\n
    # FRAME <tagged-fields>\n
    # data...FRAME <tagged-fields>\n
    # data...FRAME <tagged-fields>\n
    # data...

    with open(ref_file, 'rb') as infile, open(out_file, 'wb') as outfile:
        header = infile.readline()

        if not header.startswith(b'YUV4MPEG2'):
            raise ValueError("Not a valid Y4M file")

        outfile.write(header)

        # Parse dimensions
        header_str = header.decode()
        width = int(header_str.split('W')[1].split()[0])
        height = int(header_str.split('H')[1].split()[0])

        chroma_format = ''
        if 'C' in header_str:
            chroma_tag = header_str.split('C')[1].split()[0]
            if chroma_tag.startswith('420'):
                chroma_format = '420'
            elif chroma_tag.startswith('422'):
                chroma_format = '422'
            elif chroma_tag.startswith('444'):
                chroma_format = '444'
            elif chroma_tag.startswith('mono'):
                chroma_format = 'mono'
            else:
                raise ValueError(
                    "Could not determine chroma format from Y4M header")

        frame_pixels = width * height

        frame_data_size = 0
        if chroma_format == '420':
            # 4:2:0 - for every 4 luma -> two chroma => 0.5 chroma per luma
            frame_data_size = int(frame_pixels * 1.5)

        elif chroma_format == '422':
            # 4:2:2 - For every 2 luma -> two choma ==> 1 chroma per luma
            frame_data_size = frame_pixels * 2

        elif chroma_format == '444':
            # 4:4:4 - For every luma -> 2 chroma per luma
            frame_data_size = frame_pixels * 3

        elif chroma_format == 'mono':
            # Monochrome - luma only
            frame_data_size = frame_pixels
        else:
            raise ValueError("Unsupported chroma format")

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

        print(f"\nFinished copy: ref video has {frames_written} frames")


def calculate_quality_metrics(ref_file, input_dir, out_dir):
    dist_file = Path(input_dir) / "out.y4m"

    cam = cv2.VideoCapture(ref_file)
    fps = cam.get(cv2.CAP_PROP_FPS)

    lost_frames = get_lost_frames(f'{input_dir}/sender.stderr.log',
                                  f'{input_dir}/receiver.stderr.log')

    config = parsers.parse_json_log_no_convert(f'{input_dir}/config.json')
    duration = config['duration'][0]
    num_frames = int(fps * duration)

    if len(lost_frames) > 0:
        print("Distorted video has missing frames. Create copy of reference and remove missing frames...")

        tmp_file = Path(out_dir) / "tmp_ref_file.y4m"
        lost_frame_log = Path(out_dir) / "lost_frames.csv"

        remove_frames(Path(ref_file), lost_frames, tmp_file, num_frames)
        export_lost_frames_csv(lost_frames, lost_frame_log)

        qm = ffmpeg.FfmpegQualityMetrics(
            ref=str(tmp_file), dist=str(dist_file), framerate=fps, progress=True)
        qm.calculate()

        tmp_file.unlink()

    else:
        qm = ffmpeg.FfmpegQualityMetrics(
            ref=ref_file, dist=str(dist_file), framerate=fps, progress=True, num_frames=num_frames)

        qm.calculate()

    csv_output = qm.get_results_csv()

    with open(f"{out_dir}/video.quality.csv", "w+") as f:
        f.write(csv_output)
