# -*- coding: utf-8 -*-

import re
import datetime
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as sf

month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
             'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}


def parse_apache_time(s):

    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


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
                     .filter(lambda s: s[1] == 1)
                     .map(lambda s: s[0]))

    failed_logs_l = (parsed_logs_l
                     .filter(lambda s: s[1] == 0)
                     .map(lambda s: s[0]))

    failed_logs_count = failed_logs_l.count()
    if failed_logs_count > 0:
        print 'Number of invalid logline: %d' % failed_logs_l.count()
        for line in failed_logs_l.take(20):
            print 'Invalid logline: %s' % line

    print 'Read %d lines, successfully parsed %d lines, failed to parse %d lines' % (parsed_logs_l.count(),
                                                                                     access_logs_l.count(),
                                                                                     failed_logs_l.count())

    return parsed_logs_l, access_logs_l, failed_logs_l


sc = SparkContext(appName="Workgroup Assignment-Group D")
sqlContext = SQLContext(sc)

parsed_logs, access_logs, failed_logs = parse_logs()


##### Defining the RDD and registering as a temporal table
df_access = sqlContext.createDataFrame(access_logs)
df_access.registerTempTable("access_logs")


##### Analyzing the data

# df_access.show(1)

#ient_identd|content_size|           date_time|            endpoint|             host|method|protocol|response_code|user_id|
# +-------------+------------+--------------------+--------------------+-----------------+------+--------+-------------+-------+
#            -|        1839|1995-08-01 00:00:...|/shuttle/missions...|in24.inetnebr.com|   GET|HTTP/1.0|          200|      -|


## Question 1
## Minimum, maximum and average of response size (size)

ResponseSize = sqlContext.sql(
    "select min(content_size) as minimum,"
    "       max(content_size) as maximum,"
    "       avg(content_size) as average"
    "  from access_logs"
)

print 'Minimum, maximum and average of response size are' , ResponseSize.head()
print


## Question 2
## Number of requests for each response code

NumberRequest = sqlContext.sql(
    "select response_code, count(response_code) as Number"
    " from access_logs"
    " group by response_code"
)

print 'Number of requests for each response code' , NumberRequest.show()
print


## Question 3
## 20 hosts that have been visited more than 10 times

NumberTopHost = sqlContext.sql(
    " select host, count(*) as Number"
    " from access_logs"
    " group by host"
    " having Number > 10"
    " order by Number desc"

)

print '20 hosts that have been visited more than 10 times' , NumberTopHost.head(20)
print


## Question 4
## Top 10 most visited endpoints

NumberEndPoint = sqlContext.sql(
    " select endpoint, count(*) as Number"
    " from access_logs"
    " group by endpoint"
    " order by Number desc"
)

print 'Top 10 most visited endpoints' , NumberEndPoint.head(10)
print


## Question 5
## Top 10 failed endpoints (response_code not equals 200)

NumberFailedEndPoint = sqlContext.sql(
    " select endpoint, count(*) as Number"
    " from access_logs"
    " where response_code != '200'"
    " group by endpoint"
    " order by Number desc"
)

print 'Top 10 failed endpoints' , NumberFailedEndPoint.head(10)
print


## Question 6
## Unique hosts

Uniquehosts = sqlContext.sql(
    " select count(distinct(host)) from access_logs"
)

print 'Unique hosts' , Uniquehosts.show(10)
print


## Question 7
## Unique hosts per day

UniquehostsDay = sqlContext.sql(
    " select cast(date_time as date) as day, count(distinct(host)) as number"
    " from access_logs"
    " group by cast(date_time as date)"
)

print 'Number of Unique hosts per Day' , UniquehostsDay.head()[1]
print

## Question 8
## Average daily requests per host


AvgRequest = sqlContext.sql(
    "select cast(date_time as date) as day, host, count(host) as Number"
    " from access_logs"
    " group by cast(date_time as date), host"
)

print 'Average daily requests per host' , AvgRequest.groupBy().avg('Number').collect()
print


## Question 9
## 40 different endpoints that generate response code = 404

DifferentEndPoint = sqlContext.sql(
    " select distinct(endpoint)"
    " from access_logs"
    " where response_code = '404'"
)

print '40 different endpoints that generate response code = 404' , DifferentEndPoint.head(40)
print

## Question 10
## Top 25 endopoints that generate 404 response code

Top25EndPoint = sqlContext.sql(
    " select endpoint, count(*) as number"
    " from access_logs"
    " where response_code = '404'"
    " group by endpoint"
    " order by number desc"
)

print 'Top 25 endopoints that generate 404 response code' , Top25EndPoint.head(25)
print


## Question 11
## Top 5 days that generate 404 response code

Top5Days404 = sqlContext.sql(
    " select day(cast(date_time as date)) as day, count(*) as number"
    " from access_logs"
    " where response_code = '404'"
    " group by cast(date_time as date)"
    " order by number desc"
)

print 'Top 5 days that generate 404 response code',\
    Top5Days404.show()
