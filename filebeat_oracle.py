import getopt
from datetime import datetime, timedelta, timezone
import re
import sys
import os
import json
import http.client as httpClient
import socket

oracle_name = None
oracle_version = None
oracle_alert_file_path = None
oracle_alert_file_encode = None
elastic_host = None
elastic_port = None
elastic_index = "filebeat-oracle"
elastic_index_format = None

oracle_alert_file_timestamp_regex = None

host_hostname = socket.gethostname()
host_ip = socket.gethostbyname(host_hostname)

oracle_alert_file_timestamp_regex_10_gt = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\s[0-9]{4}$"
oracle_alert_file_timestamp_regex_10_le = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\sCST\s[0-9]{4}$"

__version__ = None
exec(open("version.py").read())


def elasticRestApiClient(esidx, esdoc):
    print(esidx)
    headers = {"Content-type": "application/json"}
    conn = httpClient.HTTPConnection(elastic_host, elastic_port)
    url_path = "/{}/_doc".format(esidx)
    conn.request("POST", url_path, esdoc, headers)
    response = conn.getresponse()
    if response.status != 201:
        print(response.read())
    conn.close()


def logTSToDatetime(timestamp):
    _format = '%a %b %d %H:%M:%S %Y'
    if oracle_version in [9, 10]:
        _format = '%a %b %d %H:%M:%S CST %Y'
    return datetime.strptime(timestamp, _format).astimezone()


def logTSMapEsidx(d):
    if elastic_index_format is None:
        return elastic_index
    return "{}-{}".format(elastic_index, d.strftime(elastic_index_format))


def esDocTemplate(utcdt, message):
    document = {
        "@timestamp": utcdt,
        "message": message,
        "log": {"flags": ["multiline"], "file": {"path": oracle_alert_file_path}},
        "host": {"hostname": host_hostname, "ip": host_ip},
        "oracle": {"version": oracle_version, "name": oracle_name},
        "agent": {"version": __version__, "type": "filebeat-oracle"}
    }
    return json.dumps(document)


def filebeatOracleClient(log):
    _logutcts = logTSToDatetime(log[0])
    esidx = logTSMapEsidx(_logutcts)
    esDoc = esDocTemplate(_logutcts.isoformat(),
                          '\n'.join(log[1:]))
    elasticRestApiClient(esidx, esDoc)


def boostrap():
    oracle_alert_file_timestamp_regex = oracle_alert_file_timestamp_regex_10_le if oracle_version in [
        9, 10] else oracle_alert_file_timestamp_regex_10_gt
    _process_lines = 0
    _valid_lines = 0
    _start = False
    _log_container = []
    _f = None

    try:
        _f = open(oracle_alert_file_path, mode='r',
                  encoding=oracle_alert_file_encode)
        while True:
            line = _f.readline()
            if not line:
                if len(_log_container) >= 1:
                    filebeatOracleClient(_log_container)
                    _valid_lines = _process_lines
                break
            _process_lines += 1
            line = line.strip('\n')
            match = re.match(oracle_alert_file_timestamp_regex, line)
            if match:
                _start = True
            if match and _start:
                if len(_log_container) >= 1:
                    filebeatOracleClient(_log_container)
                    _valid_lines = _process_lines - 1
                _log_container = []
            if _start:
                _log_container.append(line)
    finally:
        if _f is not None:
            _f.close()
        print(_valid_lines)


if __name__ == '__main__':
    oracle_name = os.environ.get("oracle_name")
    oracle_version = int(os.environ.get("oracle_version"))
    oracle_alert_file_path = os.environ.get("oracle_alert_file_path")
    oracle_alert_file_encode = os.environ.get("oracle_alert_file_encode")
    elastic_host = os.environ.get("elastic_host")
    elastic_port = int(os.environ.get("elastic_port"))
    elastic_index = os.environ.get("elastic_index")
    elastic_index_format = os.environ.get("elastic_index_format")
    boostrap()
