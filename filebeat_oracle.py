import getopt
from datetime import datetime, timedelta, timezone
import re
import sys
import os
import json
import http.client as httpClient
import socket


__version__ = "0.6.2"


host_hostname = socket.gethostname()
host_ip = socket.gethostbyname(host_hostname)


def elasticBulkApiClient():
    documents = []
    headers = {"Content-type": "application/json"}

    def post(esidx, esdoc, launch: bool):
        indexinfo = {"index": {"_index": esidx}}
        documents.append(json.dumps(indexinfo))
        documents.append(json.dumps(esdoc))
        if len(documents) >= 100 or launch:
            documents.append('')  # The bulk request must be terminated by a newline [\\\\n]
            conn = httpClient.HTTPConnection(elastic_host, elastic_port)
            conn.request("POST", "/_bulk", '\n'.join(documents), headers)
            response = conn.getresponse()
            if response.status != 200:
                print(response.read(), file=sys.stderr)
            conn.close()
    return post


def logTSToDatetime(timestamp):
    _format = '%a %b %d %H:%M:%S %Y'
    if oracle_version in [10]:
        _format = '%a %b %d %H:%M:%S CST %Y'
    return datetime.strptime(timestamp, _format).astimezone()


def logTSMapEsidx(d):
    if elastic_index_format is None or elastic_index_format == "":
        return elastic_index
    fmt = d.strftime(elastic_index_format) if '%' in elastic_index_format else elastic_index_format
    return "{}-{}".format(elastic_index, fmt)


def esDocTemplate(utcdt, message):
    return {
        "@timestamp": utcdt,
        "message": message,
        "log": {"flags": ["multiline"], "file": {"path": oracle_alert_file_path}},
        "host": {"hostname": host_hostname, "ip": host_ip},
        "oracle": {"version": oracle_version, "name": oracle_name},
        "agent": {"version": __version__, "type": "filebeat-oracle"}
    }


def filebeatOracleClient(log, esClient, launch=False):
    _logutcts = logTSToDatetime(log[0])
    esidx = logTSMapEsidx(_logutcts)
    esDoc = esDocTemplate(_logutcts.isoformat(), '\n'.join(log[1:]))
    esClient(esidx, esDoc, launch)


def main():
    _valid_lines = 0
    _start = False
    _log_container = []
    _esClient = elasticBulkApiClient()

    try:
        with open(oracle_alert_file_path, 'r', encoding=oracle_alert_file_encoding) as _f:
            for f_idx, f_line in enumerate(_f):
                log = f_line.strip('\n')
                match = re.match(oracle_alert_file_timestamp_regex, log)
                if match:
                    _start = True
                if match and _start:
                    if len(_log_container) >= 1:
                        filebeatOracleClient(_log_container, _esClient, False)
                        _valid_lines = f_idx
                    _log_container = []
                if _start:
                    _log_container.append(log)
        if len(_log_container) >= 1:
            filebeatOracleClient(_log_container, _esClient, True)
            _valid_lines = f_idx + 1
    finally:
        print(_valid_lines)


if __name__ == '__main__':
    oracle_name = os.environ.get('oracle_name')
    oracle_version = int(os.environ.get('oracle_version'))
    oracle_alert_file_path = os.environ.get('oracle_alert_file_path')
    oracle_alert_file_encoding = os.environ.get('oracle_alert_file_encoding', 'utf-8')
    elastic_host = os.environ.get('elastic_host')
    elastic_port = int(os.environ.get('elastic_port', '9200'))
    elastic_index = os.environ.get('elastic_index', 'filebeat-oracle')
    elastic_index_format = os.environ.get('elastic_index_format')

    for env in ['oracle_name', 'oracle_version', 'oracle_alert_file_path', 'elastic_host', 'elastic_index_format']:
        if os.environ.get(env) is None:
            print("env {} is Nnone.".format(env))
            sys.exit(1)

    oracle_alert_file_timestamp_regex = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\s[0-9]{4}$"
    if oracle_version in [9, 10]:
        oracle_alert_file_timestamp_regex = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\sCST\s[0-9]{4}$"
    main()
