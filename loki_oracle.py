import getopt
from datetime import datetime, timezone
import time
import re
import sys
import os
import json
import http.client as httpClient
import socket


__version__ = "0.1.0"


host_hostname = socket.gethostname()
host_ip = socket.gethostbyname(host_hostname)


def lokiApiClient(body):
    headers = {"Content-type": "application/json"}
    conn = httpClient.HTTPConnection(loki_host, loki_port)
    conn.request("POST", "/loki/api/v1/push", json.dumps(body), headers)
    response = conn.getresponse()
    if response.status != 204:
        print(response.read(), file=sys.stderr)
    conn.close()


def logTSToDatetime(timestamp):
    _format = '%a %b %d %H:%M:%S %Y'
    if db_version in [10]:
        _format = '%a %b %d %H:%M:%S CST %Y'
    return datetime.strptime(timestamp, _format).astimezone()


def logTSToUnixTimestamp(timestamp):
    return str(int(time.mktime(logTSToDatetime(timestamp).timetuple())) * 1000000000)


def lokiDocTemplate(values):
    return {
        "streams": [
            {
                "stream": {
                    "db_group": db_group,
                    "db_role": db_role,
                    "db_uname": db_uname,
                    "db_inst": db_inst,
                    "db_version": str(db_version),
                    "host_name": host_hostname,
                    "host_ip": host_ip,
                    "agent_version": __version__,
                    "agent_type": "loki_oracle",
                    "log_path": db_alert_file_path
                },
                "values": values
            }
        ]
    }


def lokiOracleClient(logs):
    values = []
    for log in logs:
        unixts = logTSToUnixTimestamp(log[0])
        content = '\n'.join(log[1:])
        values.append([unixts, content])
    postBody = lokiDocTemplate(values)
    lokiApiClient(postBody)


def main():
    _valid_lines = 0
    _start = False
    _log_container = []

    _cache_log = []

    _client = lokiOracleClient

    try:
        with open(db_alert_file_path, 'r', encoding=db_alert_file_encoding) as _f:
            for f_idx, f_line in enumerate(_f):
                log = f_line.strip('\n')
                match = re.match(db_alert_file_timestamp_regex, log)
                if match:
                    _start = True
                if match and _start:
                    if len(_log_container) >= 1:
                        _cache_log.append(_log_container)
                        _valid_lines = f_idx
                        if len(_cache_log) >= 100:
                            _client(_cache_log)
                            _cache_log.clear()
                    _log_container = []
                if _start:
                    _log_container.append(log)
        if len(_log_container) >= 1:
            _cache_log.append(_log_container)
            _client(_cache_log)
            _valid_lines = f_idx + 1
    finally:
        print(_valid_lines)


if __name__ == '__main__':
    db_group = os.environ.get('db_group')
    db_uname = os.environ.get('db_uname')
    db_role = os.environ.get('db_role')
    db_inst = os.environ.get('db_inst')
    db_version = int(os.environ.get('db_version'))
    db_alert_file_path = os.environ.get('db_alert_file_path')
    db_alert_file_encoding = os.environ.get('db_alert_file_encoding', 'utf-8')
    loki_host = os.environ.get('loki_host')
    loki_port = int(os.environ.get('loki_port', '3100'))

    for env in ['db_group', 'db_uname', 'db_role', 'db_inst', 'db_version', 'db_alert_file_path', 'loki_host']:
        if os.environ.get(env) is None:
            print("env {} is Nnone.".format(env))
            sys.exit(1)

    db_alert_file_timestamp_regex = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\s[0-9]{4}$"
    if db_version in [9, 10]:
        db_alert_file_timestamp_regex = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\sCST\s[0-9]{4}$"
    main()
