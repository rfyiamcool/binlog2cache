#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# watch mysql binlog event, update redis cache
#
import sys

import redis
import yaml

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)


def load_config(file_path):
    f = open(file_path)
    try:
        conf = yaml.load(f)
        return conf
    except:
        exit()


class SyncCache(object):
    def __init__(self, config):
        self.config = config

        # reduce var path
        self.MYSQL_SETTINGS = config["MYSQL_SETTINGS"]
        self.REDIS_SETTINGS = config["REDIS_SETTINGS"]
        self.SELF = config["SELF"]

        # init redis client
        self.redis_client = redis.StrictRedis(
            host=self.REDIS_SETTINGS["host"],
            port=self.REDIS_SETTINGS["port"],
            db=self.REDIS_SETTINGS["db"],
            password=self.REDIS_SETTINGS["password"]
        )
        

    def transfer(self):
        log_file, log_pos = self.get_log_pos()
        stream = BinLogStreamReader(
            connection_settings=self.MYSQL_SETTINGS,
            server_id=int(self.SELF["server_id"]),
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            log_file = log_file,
            log_pos = log_pos,
            blocking=True)

        print "sync"
        for binlogevent in stream:
            prefix = "%s:%s:" % (binlogevent.schema, binlogevent.table)
            self.set_log_pos(stream.log_file, stream.log_pos)

            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    self.delete_handler(prefix, row)

                elif isinstance(binlogevent, UpdateRowsEvent):
                    self.update_handler(prefix, row)

                elif isinstance(binlogevent, WriteRowsEvent):
                    self.insert_handler(prefix, row)

        stream.close()

    def delete_handler(self, prefix, row):
        vals = row["values"]
        self.redis_client.delete(prefix + str(vals["id"]))

    def update_handler(self, prefix, row):
        vals = row["after_values"]
        self.redis_client.hmset(prefix + str(vals["id"]), vals)

    def insert_handler(self, prefix, row):
        vals = row["values"]
        self.redis_client.hmset(prefix + str(vals["id"]), vals)

    def set_log_pos(self, log_file, log_pos):
        key = '%s%s' % (self.SELF['log_pos_prefix'], self.SELF['server_id'])
        self.redis_client.hmset(key, {'log_pos': log_pos, 'log_file': log_file})

    def get_log_pos(self):
        key = '%s%s' % (self.SELF['log_pos_prefix'], self.SELF['server_id'])
        ret = self.redis_client.hgetall(key)
        return self.redis_client.get('log_file'), ret.get('log_pos')


def main():
    if len(sys.argv) == 1:
        exit()

    conf = sys.argv[1]
    config = load_config(conf)
    syncer = SyncCache(config)
    syncer.transfer()


if __name__ == "__main__":
    main()