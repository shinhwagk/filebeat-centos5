import getopt
from datetime import datetime, date, time, timedelta
from string import Template
import re
import httplib
import sys

oracleVersion = None
alertFilePath = None
elasticHost = None
elasticPort = None
elasticIndex = None
alertTSRegex = None
start = False
processLines = 0
validLines = 0


def parserArgs(args):
    global oracleVersion
    global alertFilePath
    global elasticHost
    global elasticPort
    global elasticIndex
    global alertTSRegex
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
                oracleVersion = value
        elif opt in ('--alertFilePath'):
            alertFilePath = value
        elif opt == '--elasticHost':
            elasticHost = value
        elif opt == '--elasticPort':
            elasticPort = value
        elif opt == '--elasticIndex':
            elasticIndex = value

        if None in [oracleVersion, alertFilePath,
                    elasticHost, elasticPort, elasticIndex]:
            print "arg: %s required." % "ss"

        # -- oracle 10g version
        if oracleVersion <= "10":
            alertTSRegex = "^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\sCST\s[0-9]{4}$"
        else:
            alertTSRegex = "^[A-Za-z]{3}\s[A-Za-z]{3}\s[0-9]{2}\s[0-9]{2}:[0-9]{2}:[0-9]{2}\s[0-9]{4}$"


def elasticDocumentTemplate(timestamp, log):
    t = Template(
        '{"@timestamp":"${timestamp}","message":"${log}","log":{"flags":["multiline"],"file":"${filePath}"}}')
    return t.safe_substitute(timestamp=timestamp, log=log, filePath=alertFilepath)


def elasticTimestampTemplate(y, m, d, h, mi, ss):
    d = date(y, m, d)
    t = time(h, mi, ss)
    ts = datetime.combine(d, t) + timedelta(hours=-8)
    return ts.strftime("%Y%m%dT%H:%M:%S.000Z")


def httpPost(body):
    headers = {"Content-type": "application/json"}
    conn = httplib.HTTPConnection(elasticHost, elasticPort)
    urlPath = "/%s/_doc" % (elasticIndex)
    conn.request("POST", urlPath, body, headers)
    response = conn.getresponse()
    print response.status
    conn.close()


def abbrMonthToNumMonth(strmon):
    mon = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul',
           'Aug', 'Sep', 'Oct', 'Nov', 'Dec'].index(strmon) + 1
    return mon


def logTimestampToElasticTimestamp(timestamp):
    mon = abbrMonthToNumMonth(timestamp[4:7])
    day = timestamp[8:10]
    hour = timestamp[11:13]
    minu = timestamp[14:16]
    sec = timestamp[17:19]
    if oracleVersion == '10':
        year = timestamp[24:28]
    else:
        year = timestamp[20:24]
    return elasticTimestampTemplate(int(year), mon, int(day), int(hour), int(minu), int(sec))


def postElastic(logs):
    timestamp = logTimestampToElasticTimestamp(logs[0])
    body = elasticDocumentTemplate(timestamp, '\\n'.join(logs[1:]))
    httpPost(body)


# if __name__ == "__main__":
#     try:
#         logContainer = []
#         f = open('alert111.log', 'r')
#         while True:
#             line = f.readline()
#             if not line:
#                 break
#             processLines += 1
#             match = re.match(alertTSRegex, line)
#             if match:
#                 start = True
#             if match and start:
#                 if len(logContainer) >= 1:
#                     postElastic(logContainer)
#                     validLines = processLines - 1
#                 logContainer = []
#             if start:
#                 logContainer.append(line)
#         print validLines
#     finally:
#         f.close()


# def main():
    # parser = create_parser()
    # args = parser.parse_args()
    # ping(args.tags, args.region, args.ami)


if __name__ == '__main__':
    parserArgs(sys.argv[1:])
    # #
    # print oracleVersion
