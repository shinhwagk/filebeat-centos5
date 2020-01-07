import getopt
from datetime import datetime, timedelta, timezone
import re
import sys
import os
import json
import http.client as httpClient


def parser_args(args):
    ORACLE_VERSION = None
    ALERT_FILE_PATH = None
    ELASTIC_HOST = None
    ELASTIC_PORT = None
    ELASTIC_INDEX = "filebeat-oracle"
    ELASTIC_INDEX_FORMAT = r"%Y.%m.%d"
    ALERT_FILE_ENCODE = "utf-8"

    try:
        opts, args = getopt.getopt(args,  "", [
            "help", "oracleVersion=", "alertFilePath=", "elasticHost=",
            "elasticPort=", "elasticIndex=", "elasticIndexDateFormat=", "alertFileEncode="])
    except getopt.GetoptError:
        # usage()
        sys.exit(2)
    for opt, value in opts:
        if opt in ["--oracleVersion"]:
            if value in ["9", "10", "11", "12"]:
                ORACLE_VERSION = int(value)
        elif opt in ('--alertFilePath'):
            ALERT_FILE_PATH = value
        elif opt == '--elasticHost':
            ELASTIC_HOST = value
        elif opt == '--elasticPort':
            ELASTIC_PORT = value
        elif opt == '--elasticIndex':
            ELASTIC_INDEX = value
        elif opt == '--elasticIndexDateFormat':
            ELASTIC_INDEX_FORMAT = value
        elif opt == '--alertFileEncode':
            ALERT_FILE_ENCODE = value

    ALERT_TS_REGEX = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\s[0-9]{4}$"

    if ORACLE_VERSION in [9, 10]:
        ALERT_TS_REGEX = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\sCST\s[0-9]{4}$"

    if None in [ORACLE_VERSION, ALERT_FILE_PATH,
                ELASTIC_HOST, ELASTIC_PORT, ELASTIC_INDEX]:
        print("arg: %s required." % "ss")
        sys.exit(2)

    return ORACLE_VERSION, ALERT_FILE_PATH, ALERT_TS_REGEX, ELASTIC_HOST, ELASTIC_PORT, ELASTIC_INDEX, ELASTIC_INDEX_FORMAT, ALERT_FILE_ENCODE


def ESServicePost(elastic_host, elastic_port):
    def post(esidx, esdoc):
        headers = {"Content-type": "application/json"}
        conn = httpClient.HTTPConnection(elastic_host, elastic_port)
        url_path = "/{}/_doc".format(esidx)
        conn.request("POST", url_path, esdoc, headers)
        response = conn.getresponse()
        if response.status != 201:
            print(response.read())
        conn.close()
    return post


def logTSToDatetime(oracle_version, timestamp):
    _format = '%a %b %d %H:%M:%S %Y'
    if oracle_version in [9, 10]:
        _format = '%a %b %d %H:%M:%S CST %Y'
    return datetime.strptime(timestamp, _format).astimezone()


def logTSMapEsidx(esidx, d, fmt):
    return "{}-{}".format(esidx, d.strftime(fmt))


def esDocTemplate(utcdt, message, file_path, oracle_version):
    document = {
        "@timestamp": utcdt,
        "message": message,
        "log": {"flags": ["multiline"], "file": file_path},
        "oracle": {"version": oracle_version}
    }
    return json.dumps(document)


def postes(elastic_host, elastic_port, elastic_prefix_index, oracle_version, file_path, ELASTIC_INDEX_FORMAT):
    _ess = ESServicePost(elastic_host, elastic_port)

    def post(log):
        _logutcts = logTSToDatetime(oracle_version, log[0])
        esidx = logTSMapEsidx(elastic_prefix_index,
                              _logutcts, ELASTIC_INDEX_FORMAT)
        esDoc = esDocTemplate(_logutcts.isoformat(),
                              '\n'.join(log[1:]), file_path, oracle_version)
        _ess(esidx, esDoc)
    return post

# external shell opertaions
# def sedCmdDelFileLines(file, line):
#     command = "sed -i '1,{}d' {}".format(line, file)
#     ext = os.system(command)
#     if ext != 0:
#         print("delete line 1 - {}, exit code {}".format(line, ext))


def main(args):
    ORACLE_VERSION, ALERT_FILE_PATH, ALERT_TS_REGEX, ELASTIC_HOST, ELASTIC_PORT, ELASTIC_INDEX, ELASTIC_INDEX_FORMAT, ALERT_FILE_ENCODE = parser_args(
        args)

    _ess_client = postes(ELASTIC_HOST, ELASTIC_PORT,
                         ELASTIC_INDEX, ORACLE_VERSION, ALERT_FILE_PATH, ELASTIC_INDEX_FORMAT)

    _process_lines = 0
    _valid_lines = 0
    _start = False
    _log_container = []
    _f = None

    try:
        _f = open(ALERT_FILE_PATH, mode='r', encoding=ALERT_FILE_ENCODE)
        while True:
            line = _f.readline()
            if not line:
                if len(_log_container) >= 1:
                    _ess_client(_log_container)
                    _valid_lines = _process_lines
                break
            _process_lines += 1
            line = line.strip('\n')
            match = re.match(ALERT_TS_REGEX, line)
            if match:
                _start = True
            if match and _start:
                if len(_log_container) >= 1:
                    _ess_client(_log_container)
                    _valid_lines = _process_lines - 1
                _log_container = []
            if _start:
                _log_container.append(line)
    finally:
        if _f is not None:
            _f.close()
    if _valid_lines >= 1:
        print(_valid_lines)


if __name__ == '__main__':
    main(sys.argv[1:])
