import getopt
from datetime import datetime, timedelta
from string import Template
import re
import httplib
import sys
import os

ORACLE_VERSION = None
ALERT_FILE_PATH = None
ELASTIC_HOST = None
ELASTIC_PORT = None
ELASTIC_INDEX = "orclalertlog"
ALERT_TS_REGEX = None

# PROCESS_LINES = 0
# VALID_LINES = 0


def parser_args(args):
    global ORACLE_VERSION
    global ALERT_FILE_PATH
    global ELASTIC_HOST
    global ELASTIC_PORT
    global ELASTIC_INDEX

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

    if None in [ORACLE_VERSION, ALERT_FILE_PATH,
                ELASTIC_HOST, ELASTIC_PORT, ELASTIC_INDEX]:
        print ELASTIC_PORT
        print "arg: %s required." % "ss"


def init_global_variables():
    global ALERT_TS_REGEX
    if ORACLE_VERSION <= "10":
        ALERT_TS_REGEX = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\sCST\s[0-9]{4}$"
    else:
        ALERT_TS_REGEX = r"^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\s[0-9]{4}$"


def elasticDocumentTemplate(timestamp, log):
    _t = Template(
        '{"@timestamp":"${timestamp}","message":"${log}","log":{"flags":["multiline"],"file":"${filePath}"},"oracle":{"version":"${version}"}}')
    return _t.safe_substitute(timestamp=timestamp, log=log, filePath=ALERT_FILE_PATH, version=ORACLE_VERSION)


def elasticTimestampTemplate(y, m, d, h, mi, ss):
    return (datetime(y, m, d, h, mi, ss) + timedelta(hours=-8)).strftime("%Y%m%dT%H:%M:%S.000Z")


def httpPost(host, port, index, body):
    headers = {"Content-type": "application/json"}
    conn = httplib.HTTPConnection(host, port)
    url_path = "/%s/_doc" % (index)
    conn.request("POST", url_path, body, headers)
    response = conn.getresponse()
    if response.status != 201:
        print response.read()
    conn.close()


def abbr_month_to_num_month(strmon):
    mon = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul',
           'Aug', 'Sep', 'Oct', 'Nov', 'Dec'].index(strmon) + 1
    return mon


def logTimestampToSingleDateNumbers(timestamp):
    mon = abbr_month_to_num_month(timestamp[4:7])
    day = timestamp[8:10]
    hour = timestamp[11:13]
    minu = timestamp[14:16]
    sec = timestamp[17:19]
    if ORACLE_VERSION == '10':
        year = timestamp[24:28]
    else:
        year = timestamp[20:24]
    return int(year), mon, int(day), int(hour), int(minu), int(sec)


def logTimestampToElasticIndexName(esidx, year, month, day):
    return "%s-%s.%s.%s" % (esidx, year, month, day)


def postElastic(logs):
    year, mon, day, hour, minu, sec = logTimestampToSingleDateNumbers(logs[0])
    ests = elasticTimestampTemplate(year, mon, day, hour, minu, sec)
    body = elasticDocumentTemplate(ests, r'\n'.join(logs[1:]))
    esidx = logTimestampToElasticIndexName(ELASTIC_INDEX, year, mon, day)
    httpPost(ELASTIC_HOST, ELASTIC_PORT, esidx, body)


def main(args):
    parser_args(args)
    init_global_variables()

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
                    postElastic(_log_container)
                    _valid_lines = _process_lines
                break
            _process_lines += 1
            line = line.strip('\n')
            match = re.match(ALERT_TS_REGEX, line)
            if match:
                _start = True
            if match and _start:
                if len(_log_container) >= 1:
                    postElastic(_log_container)
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
