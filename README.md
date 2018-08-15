# binlog2cache

watch mysql binlog event, update redis cache

## use

1. dep

```
pip install -r requirements.txt
```

2. modify config.py

3. python binlog2cache.py

4. redis-cli monitor



## info

mysql data

```
+-------+-------------+------+-----+---------+----------------+
| Field | Type        | Null | Key | Default | Extra          |
+-------+-------------+------+-----+---------+----------------+
| id    | int(11)     | NO   | PRI | NULL    | auto_increment |
| pid   | int(11)     | YES  |     | NULL    |                |
| cid   | int(11)     | YES  |     | NULL    |                |
+-------+-------------+------+-----+---------+----------------+
```

redis cache data

```
hash

key --> db:table:id:pid
field --> pid
value --> xxx

field --> cid
value --> xxx
```

### mysqld config

open binlog and set binlog-format

```
[mysqld]
server-id = 1
log_bin = /var/log/mysql/mysql-bin.log
expire_logs_days = 10
max_binlog_size = 1000M
binlog-format = row
```
