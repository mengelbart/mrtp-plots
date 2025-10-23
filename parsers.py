import json
import re

import pandas as pd
import pyshark


def parse_json_log(log_file):
    with open(log_file, 'r') as f:
        data = _read_json_lines(f)

    df = pd.json_normalize(data)
    return df


def parse_pion_sctp_log(log_file):
    with open(log_file, 'r') as f:
        data = [line for line in f if line.startswith("sctp TRACE:")]

    # extract pion sctp cwnd updates - only there if webrtc data channel used
    cwnd_records = []
    min_date = pd.Timestamp.min.date()
    for line in data:
        if "updated cwnd" in line:
            # Example: sctp TRACE: 14:20:53.593906 association.go:1805: [0xc0002c61e0] updated cwnd=29996 ssthresh=1048576 acked=2048 (SS)
            m = re.search(
                r"(\d{2}:\d{2}:\d{2}\.\d{6}).*updated cwnd=(\d+)", line)
            if m:
                ts, cwnd = m.group(1), int(m.group(2))
                time_str = f"{min_date} {ts}"
                time_val = pd.to_datetime(time_str)

                cwnd_records.append(
                    {'pion-time': time_val, 'cwnd': cwnd, 'msg': "pion-sctp-cwnd"})

    if cwnd_records:
        return pd.DataFrame(cwnd_records)
    return pd.DataFrame()


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
    dtls_data = []

    def append_pkt(packet):
        if 'UDP' in packet and 'RTP' in packet and packet['rtp'].version.value == 2:
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
        if 'DTLS' in packet and 'handshake' not in packet['dtls'].field_names:
            dtls_data.append({
                'time': packet.frame_info.time_epoch,
                'src': packet['ip'].src.value,
                'dst': packet['ip'].dst.value,
                'src_port': packet['udp'].srcport.value,
                'dst_port': packet['udp'].dstport.value,
                'length': packet['dtls'].record.length.value,
                'seq': packet['dtls'].record.sequence.number.value,
            })

    pcap = pyshark.FileCapture(pcap_file, include_raw=True, use_ek=True)
    await pcap.packets_from_tshark(append_pkt)

    rtp_df = pd.DataFrame()
    rtcp_df = pd.DataFrame()
    dtls_df = pd.DataFrame()

    if len(rtp_data) > 0:
        rtp_df = pd.DataFrame(rtp_data)
        rtp_df['time'] = pd.to_datetime(rtp_df['time'], format='ISO8601')
        rtp_df = rtp_df.set_index('time')

    if len(rtcp_data) > 0:
        rtcp_df = pd.DataFrame(rtcp_data)
        rtcp_df['time'] = pd.to_datetime(rtcp_df['time'], format='ISO8601')
        rtcp_df = rtcp_df.set_index('time')

    if len(dtls_data) > 0:
        dtls_df = pd.DataFrame(dtls_data)
        dtls_df['time'] = pd.to_datetime(dtls_df['time'], format='ISO8601')
        dtls_df = dtls_df.set_index('time')

    return rtp_df, rtcp_df, dtls_df
