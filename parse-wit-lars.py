#!/usr/bin/python3

import io, sys
from lars import apache

def read_log_file_into_list(filename):
    lines = []
    with io.open('sample.log') as f:
        lines = f.read().splitlines()
    return lines

def convert_list_to_stream_object(list_log_data):
    return io.StringIO('\n'.join(list_log_data))

def process_log(file_like_log_data):
    status_codes = {}
    ips_by_volume = {}
    with apache.ApacheSource(file_like_log_data) as source:
        for row in source:
            #print(str(row.remote_host) + " - " + str(row.status) + " - " + str(row.time.hour))
            if not row.status in status_codes:
                status_codes[row.status] = 1
            else:
                status_codes[row.status] += 1

            if not row.remote_host.compressed in ips_by_volume:
                ips_by_volume[row.remote_host.compressed] = 1
            else:
                ips_by_volume[row.remote_host.compressed] += 1

    # Sort ips_by_volume by value to get top 5.
    sorted_ips_by_volume = sorted(ips_by_volume.items(), key=lambda x:x[1])

    return (status_codes, sorted_ips_by_volume)


def main() -> int:
    log_filename = 'sample.log'

    log_list_data = read_log_file_into_list(log_filename)
    log_stream_obj = convert_list_to_stream_object(log_list_data)

    status_codes, ips_by_volume = process_log(log_stream_obj)

    # Output
    print("Breakdown of status codes:")
    codes = list(status_codes.keys())
    codes.sort()
    for code in codes:
        print("code " + str(code) + " = " + str(status_codes[code]))

    print("\nTop 5 ips by volume:")
    for ip in ips_by_volume[-5:]:
        print(ip[0] + " => " + str(ip[1]))

    return 0

if __name__ == '__main__':
    sys.exit(main())

