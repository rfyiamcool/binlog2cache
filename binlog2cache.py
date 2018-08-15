#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# watch mysql binlog event, update redis cache
#

import redis

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)


MYSQL_SETTINGS = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": ""
}

REDIS_SETTINGS = {
    "host": "127.0.0.1",
    "port": 6379,
    "db": 0,
    "password": "",
}

config = {
    'server_id': 11,
    'log_pos_prefix': 'log_pos_'
}


r = redis.StrictRedis(
    host=REDIS_SETTINGS["host"],
    port=REDIS_SETTINGS["port"],
    db=REDIS_SETTINGS["db"],
    password=''
)


def main():
    log_file, log_pos = get_log_pos()
    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=11,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
        #log_file = "mysql-bin.000001",
        #log_pos = "12812",
        blocking=True)

    print "sync"
    for binlogevent in stream:
        prefix = "%s:%s:" % (binlogevent.schema, binlogevent.table)
        set_log_pos(stream.log_file, stream.log_pos)

        for row in binlogevent.rows:
            if isinstance(binlogevent, DeleteRowsEvent):
                vals = row["values"]
                r.delete(prefix + str(vals["id"]))

            elif isinstance(binlogevent, UpdateRowsEvent):
                vals = row["after_values"]
                r.hmset(prefix + str(vals["id"]), vals)

            elif isinstance(binlogevent, WriteRowsEvent):
                vals = row["values"]
                r.hmset(prefix + str(vals["id"]), vals)

    stream.close()


def set_log_pos(log_file, log_pos):
    key = '%s%s' % (config['log_pos_prefix'], config['server_id'])
    r.hmset(key, {'log_pos': log_pos, 'log_file': log_file})


def get_log_pos():
    key = '%s%s' % (config['log_pos_prefix'], config['server_id'])
    ret = r.hgetall(key)
    return ret.get('log_file'), ret.get('log_pos')


if __name__ == "__main__":
    main()
