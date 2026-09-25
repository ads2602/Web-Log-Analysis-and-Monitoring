import re
import json
from datetime import datetime


LOG_PATTERN = r'(?P<ip>[\d.]+) - - \[(?P<timestamp>.*?)\] "(?P<method>[A-Z]+) (?P<url>.*?) HTTP/.*?" (?P<status>\d{3}) (?P<bytes>\d+|-) "(?P<referrer>.*?)" "(?P<user_agent>.*?)"'


def parse_log_line(line):
    match = re.match(LOG_PATTERN, line)
    if match:
        log_data = match.groupdict()

        log_data['timestamp'] = datetime.strptime(
            log_data['timestamp'], '%d/%b/%Y:%H:%M:%S %z'
        ).isoformat()

        log_data['bytes'] = int(log_data['bytes']) if log_data['bytes'] != '-' else 0
        return log_data
    return None


def preprocess_logs(input_file, output_file):
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            parsed_log = parse_log_line(line.strip())
            if parsed_log:
                json.dump(parsed_log, outfile)
                outfile.write('\n')


input_file = '/app/data/access.log'
output_file = '/app/data/processed_logs.json'
preprocess_logs(input_file, output_file)
