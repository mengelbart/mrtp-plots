import json
from math import log

import pandas as pd
import pyshark


def parse_json_log(log_file):
    with open(log_file, 'r') as f:
        data = _read_json_lines(f)

    df = pd.json_normalize(data)
    return df


def parse_qlog(log_file):
    with open(log_file, 'r') as f:
        data = _read_json_lines(f)

    reference_time = data[0]['trace']["common_fields"]['reference_time']
    df = pd.json_normalize(data)

    # add reference time to all relative timestamps
    df["time"] = pd.to_datetime(df["time"] + reference_time, unit="ms")
    return df


def _read_json_lines(log_file):
    data = []
    for line in log_file:
        line = line.strip()
        if not line:
            continue
        try:
            data.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return data


async def parse_pcap(pcap_file):
    rtp_data = []
    rtcp_data = []

    def append_pkt(packet):
        if 'UDP' in packet and 'RTP' in packet and packet['rtp'].version == 2:
            rtp_data.append({
                'time': packet.frame_info.time_epoch,
                'src': packet['ip'].src.value,
                'dst': packet['ip'].dst.value,
                'src_port': packet['udp'].srcport.value,
                'dst_port': packet['udp'].dstport.value,
                'length': packet['udp'].length.value,
                'rtp_ts': packet['rtp'].timestamp.value,
                'seq': packet['rtp'].seq.value,
                'extseq': packet['rtp'].extseq.value,
                'ssrc': packet['rtp'].ssrc.value,
                'marker': packet['rtp'].marker.value,
            })
        if 'UDP' in packet and ('RTCP' in packet or 'SRTCP' in packet):
            rtcp_data.append({
                'time': packet.frame_info.time_epoch,
                'src': packet['ip'].src.value,
                'dst': packet['ip'].dst.value,
                'src_port': packet['udp'].srcport.value,
                'dst_port': packet['udp'].dstport.value,
                'length': packet['udp'].length.value,
            })

    pcap = pyshark.FileCapture(pcap_file, include_raw=True, use_ek=True)
    await pcap.packets_from_tshark(append_pkt)

    if len(rtp_data) == 0 or len(rtcp_data) == 0:
        return pd.DataFrame(), pd.DataFrame()

    rtp_df = pd.DataFrame(rtp_data)
    rtp_df['time'] = pd.to_datetime(rtp_df['time'], format='ISO8601')
    rtp_df = rtp_df.set_index('time')

    rtcp_df = pd.DataFrame(rtcp_data)
    rtcp_df['time'] = pd.to_datetime(rtcp_df['time'], format='ISO8601')
    rtcp_df = rtcp_df.set_index('time')

    return rtp_df, rtcp_df
