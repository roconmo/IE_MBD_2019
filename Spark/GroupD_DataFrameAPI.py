# -*- coding: utf-8 -*-
from pyspark.sql.functions import dayofmonth
import re
import datetime
import os
from pyspark.sql import GroupedData
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as sf
from pyspark.sql.types import IntegerType

month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
             'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}


def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[ 7:11 ]),
                             month_map[ s[ 3:6 ] ],
                             int(s[ 0:2 ]),
                             int(s[ 12:14 ]),
                             int(s[ 15:17 ]),
                             int(s[ 18:20 ]))


# A regular expression pattern to extract fields from the log line
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*) ?" (\d{3}) (\S+)'


def parse_apache_log_line(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return logline, 0
    size_field = match.group(9)
    if size_field == '-':
        size = long(0)
    else:
        size = long(match.group(9))
    return (Row(
        host=match.group(1),
        client_identd=match.group(2),
        user_id=match.group(3),
        date_time=parse_apache_time(match.group(4)),
        method=match.group(5),
        endpoint=match.group(6),
        protocol=match.group(7),
        response_code=int(match.group(8)),
        content_size=size
    ), 1)


def parse_logs():
    """ Read and parse log file """
    log_file = os.path.join('data', 'apache.access.log')
    parsed_logs_l = (sc
                     .textFile(log_file)
                     .map(parse_apache_log_line))

    access_logs_l = (parsed_logs_l
                     .filter(lambda s: s[ 1 ] == 1)
                     .map(lambda s: s[ 0 ]))

    failed_logs_l = (parsed_logs_l
                     .filter(lambda s: s[ 1 ] == 0)
                     .map(lambda s: s[ 0 ]))

    failed_logs_count = failed_logs_l.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs_l.count()
        for line in failed_logs_l.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs_l.count(),
                                                                                     access_logs_l.count(),
                                                                                     failed_logs_l.count())

    return parsed_logs_l, access_logs_l, failed_logs_l


sc = SparkContext(appName="Workgroup Assignment-Group A")
sqlContext = SQLContext(sc)

parsed_logs, access_logs, failed_logs = parse_logs()

##### Defining the RDD and registering as a temporal table
df_parsed = sqlContext.createDataFrame(access_logs)
df_parsed.registerTempTable("access_logs")
df = df_parsed  # type: object

##  Minimum, maximum and average of response size

print'1- Minimum, maximum and average of response size '
#df.describe([ 'content_size' ]).filter(sf.col('mean')).show()

df.agg(sf.avg(df.content_size).alias('Average'), sf.min(df.content_size).alias('Min'), sf.max(df.content_size).alias('Max')).show()



## Number of requests for each response code

print'2- Number of requests for each response code'
df.groupby(['response_code']).count().show()




## 20 hosts that have been visited more than 10 times

print'3- 20 hosts that have been visited more than 10 times'


df.groupBy(['host']).count().distinct().filter(sf.col('count')>'10').orderBy(sf.col('count'),ascending=False).show(20)

## Top 10 most visited endpoints
print'4- Top 10 most visited endpoints'

visit = df.groupBy(['endpoint']).count().distinct().sort(sf.col('count').desc()).show(10)


## Top 10 failed endpoints (response_code not equals 200)

print'5- Top 10 failed endpoints (response_code not equals 200)'

res_df = df.filter(df.response_code != 200 )

fail_df = res_df.groupBy(['endpoint']).count().sort(sf.col('count').desc()).show(10)

## Unique hosts
print'6- Unique hosts'

df.select(sf.countDistinct("host")).show()


## Unique hosts per day

print'7- Unique hosts per day'

day = df.select(df.host, dayofmonth('date_time').alias('day'))
day_group = (day.dropDuplicates())
daily_hosts = (day_group .groupBy('day') .count().alias('count').cache())

daily_hosts.show()


## Average daily requests per host

print'8- Average daily requests per host'
df.groupBy('host').count().agg({'count': 'mean'}).show()

## 40 different endpoints that generate response code = 404

print '9- 40 different endpoints that generate response code = 404'

df.select(df.endpoint).distinct().filter(df["response_code"] == 404).show(40)


## Top 25 endopoints that generate 404 response code

print ' 10- Top 25 endopoints that generate 404 response code'

res =  df.filter(df["response_code"] == 404)
res_1 = res.select(res.endpoint)
top_25 = res_1.groupBy("endpoint").count().distinct().sort("count", ascending=False).show(25)


## Top 5 days that generate 404 response code

print '11 - Top 5 days that generate 404 response code'

res.groupBy(dayofmonth('date_time').alias('day')).count().sort("day").show()

