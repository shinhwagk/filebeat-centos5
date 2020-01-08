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
oracle_alert_file_encode = "utf-8"
elastic_host = None
elastic_port = 9200
elastic_index = "filebeat-oracle"
elastic_index_format = None

oracle_alert_file_timestamp_regex = None

host_hostname = socket.gethostname()
host_ip = socket.gethostbyname(host_hostname)

oracle_alert_file_timestamp_regex_10_gt = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\s[0-9]{4}$"
oracle_alert_file_timestamp_regex_10_le = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\sCST\s[0-9]{4}$"

__version__ = None
exec(open("version.py").read())


def parser_args_to_global_vars(args):
    global oracle_name, oracle_version, oracle_alert_file_path, oracle_alert_file_encode, elastic_host, elastic_port, elastic_index, elastic_index_format

    try:
        opts, args = getopt.getopt(args,  "", ["help", "oracleName=", "oracleVersion=", "oracleAlertFilePath=", "oracleAlertFileEncode=", "elasticHost=",
                                               "elasticPort=", "elasticIndex=", "elasticIndexFormat="])
    except getopt.GetoptError:
        # usage()
        sys.exit(2)
    for opt, value in opts:
        if opt == '--oracleName':
            oracle_name = value
        elif opt == "--oracleVersion":
            if value in ["10", "11", "12", "18", "19"]:
                oracle_version = int(value)
        elif opt == '--oracleAlertFilePath':
            oracle_alert_file_path = value
        elif opt == '--oracleAlertFileEncode':
            oracle_alert_file_encode = value
        elif opt == '--elasticHost':
            elastic_host = value
        elif opt == '--elasticPort':
            elastic_port = value
        elif opt == '--elasticIndex':
            elastic_index = value
        elif opt == '--elasticIndexFormat':
            elastic_index_format = value

    if None in [oracle_name, oracle_version, oracle_alert_file_path, elastic_host]:
        sys.exit(2)


def elasticRestApiClient(esidx, esdoc):
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
    if oracle_version in [10]:
        _format = '%a %b %d %H:%M:%S CST %Y'
    return datetime.strptime(timestamp, _format).astimezone()


def logTSMapEsidx(d):
    if elastic_index_format is None or elastic_index_format == "":
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
    esDoc = esDocTemplate(_logutcts.isoformat(), '\n'.join(log[1:]))
    elasticRestApiClient(esidx, esDoc)


def main(argv):

    parser_args_to_global_vars(argv)

    if oracle_version in [9, 10]:
        oracle_alert_file_timestamp_regex = oracle_alert_file_timestamp_regex_10_le
    else:
        oracle_alert_file_timestamp_regex = oracle_alert_file_timestamp_regex_10_gt

    _process_lines = 0
    _valid_lines = 0
    _start = False
    _log_container = []
    _f = None

    try:
        _f = open(oracle_alert_file_path, 'r',
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
    main(sys.argv[1:])
