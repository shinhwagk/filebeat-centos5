# filebeat-oracle
https://pypi.org/project/filebeat-oracle/#files


```
/var/lib/dbus/machine-id
```


```sh
curl -XDELETE http://ELASTIC:9200/oracle_alert_log

cp test/10.log test/10.1.log
python filebeat_oracle.py --oracleVersion 10 --alertFilePath ./test/10.log --elasticHost elastic --elasticPort 9200 --elasticIndexDateFormat "%Y"
```

### parms
- elasticIndexDateFormat
> python lib datetime.strptime
