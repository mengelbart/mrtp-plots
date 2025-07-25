import json

import pandas as pd
import pyshark


def parse_json_log(log_file):
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
    return df


async def parse_pcap(pcap_file):
    rtp_data = []
    rtcp_data = []

    def append_pkt(packet):
        if 'UDP' in packet and 'RTP' in packet:
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

    rtp_df = pd.DataFrame(rtp_data)
    rtp_df['time'] = pd.to_datetime(rtp_df['time'], format='ISO8601')
    rtp_df = rtp_df.set_index('time')

    rtcp_df = pd.DataFrame(rtcp_data)
    rtcp_df['time'] = pd.to_datetime(rtcp_df['time'], format='ISO8601')
    rtcp_df = rtcp_df.set_index('time')

    return rtp_df, rtcp_df
