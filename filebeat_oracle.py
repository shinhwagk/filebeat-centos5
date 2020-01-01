import getopt
from datetime import datetime, timedelta
from string import Template
import re
import httplib
import sys
import os


def parser_args(args):
    ORACLE_VERSION = None
    ALERT_FILE_PATH = None
    ELASTIC_HOST = None
    ELASTIC_PORT = None
    ELASTIC_INDEX = "filebeat-oracle"
    ALERT_TS_REGEX = None
    try:
        opts, args = getopt.getopt(args,  "", [
            "help", "oracleVersion=", "alertFilePath=", "elasticHost=",
            "elasticPort=", "elasticIndex="])
    except getopt.GetoptError:
        # usage()
        sys.exit(2)
    for opt, value in opts:
        if opt in ["--oracleVersion"]:
            if value in ["9", "10", "11", "12"]:
                ORACLE_VERSION = value
        elif opt in ('--alertFilePath'):
            ALERT_FILE_PATH = value
        elif opt == '--elasticHost':
            ELASTIC_HOST = value
        elif opt == '--elasticPort':
            ELASTIC_PORT = value
        elif opt == '--elasticIndex':
            ELASTIC_INDEX = value

    if ORACLE_VERSION in ["9", "10"]:
        ALERT_TS_REGEX = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\sCST\s[0-9]{4}$"
    else:
        ALERT_TS_REGEX = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\s[0-9]{4}$"

    if None in [ORACLE_VERSION, ALERT_FILE_PATH,
                ELASTIC_HOST, ELASTIC_PORT, ELASTIC_INDEX]:
        print ELASTIC_PORT
        print "arg: %s required." % "ss"
        sys.exit(2)

    return ORACLE_VERSION, ALERT_FILE_PATH, ALERT_TS_REGEX, ELASTIC_HOST, ELASTIC_PORT, ELASTIC_INDEX


def ESServicePost(ELASTIC_HOST, ELASTIC_PORT):
    def post(esidx, doc):
        headers = {"Content-type": "application/json"}
        conn = httplib.HTTPConnection(ELASTIC_HOST, ELASTIC_PORT)
        url_path = "/%s/_doc" % (esidx)
        conn.request("POST", url_path, doc, headers)
        response = conn.getresponse()
        if response.status != 201:
            print response.read()
        conn.close()
    return post


def abbr_month_to_num_month(smon):
    return ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul',
            'Aug', 'Sep', 'Oct', 'Nov', 'Dec'].index(smon) + 1


def parseLogTSToSingleDateNums(oracle_version, timestamp):
    mon = abbr_month_to_num_month(timestamp[4:7])
    day = timestamp[8:10]
    hour = timestamp[11:13]
    minu = timestamp[14:16]
    sec = timestamp[17:19]
    if oracle_version in ['9', '10']:
        year = timestamp[24:28]
    else:
        year = timestamp[20:24]
    return int(year), mon, int(day), int(hour), int(minu), int(sec)


def logTSMapEsidx(esidx, year, month, day):
    return "%s-%s.%s.%s" % (esidx, year, month, day)


def esDocTSTemplate(y, m, d, h, mi, ss):
    return (datetime(y, m, d, h, mi, ss) + timedelta(hours=-8)).strftime("%Y%m%dT%H:%M:%S.000Z")


def esDocTemplate(timestamp, log, file_path, oracle_version):
    _t = Template(
        '{"@timestamp":"${timestamp}","message":"${log}","log":{"flags":["multiline"],"file":"${filePath}"},"oracle":{"version":"${version}"}}')
    return _t.safe_substitute(timestamp=timestamp, log=log, filePath=file_path, version=oracle_version)


def postes(elastic_host, elastic_port, elastic_prefix_index, oracle_version, file_path):
    _ess = ESServicePost(elastic_host, elastic_port)

    def post(log):
        year, mon, day, hour, minu, sec = parseLogTSToSingleDateNums(
            oracle_version, log[0])
        esidx = logTSMapEsidx(elastic_prefix_index, year, mon, day)
        esDocTimestamp = esDocTSTemplate(year, mon, day, hour, minu, sec)
        esDoc = esDocTemplate(
            esDocTimestamp, r'\n'.join(log[1:]), file_path, oracle_version)
        _ess(esidx, esDoc)
    return post


def main(args):
    ORACLE_VERSION, ALERT_FILE_PATH, ALERT_TS_REGEX, ELASTIC_HOST, ELASTIC_PORT, ELASTIC_INDEX = parser_args(
        args)

    _ess_client = postes(ELASTIC_HOST, ELASTIC_PORT,
                         ELASTIC_INDEX, ORACLE_VERSION, ALERT_FILE_PATH)

    _process_lines = 0
    _valid_lines = 0
    _start = False
    _log_container = []
    _f = None

    try:
        _f = open(ALERT_FILE_PATH, 'r')
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
        command = "sed -i '1,%sd' %s" % (_valid_lines, ALERT_FILE_PATH)
        ext = os.system(command)
        if ext <> 0:
            print "delete line 1 - %s, exit code %s" % (_valid_lines, ext)


if __name__ == '__main__':
    main(sys.argv[1:])
