'''
File:     db_manager.py
Author:   Quangtri Thai
Contents: Class to access and query Postgres.
'''

import psycopg2
import csv
import time

# Manages the psql database, handling inserts, deletes, etc.
class DBManager():
  def __init__(self, user, password, host, database, sql_log_file="sql_log.SQL", csv_file="log_batch.csv", clear_log=True):
    self.user = user
    self.password = password
    self.host = host
    self.database = database
    self.db_conn = psycopg2.connect(user=self.user,
                                    password=self.password,
                                    host=self.host,
                                    database=self.database)
    self.sql_log_file = sql_log_file
    self.csv_file = csv_file

    if clear_log: open(self.sql_log_file, "w").close()

  def insert_log(self, table, log):
    sql_log = open(self.sql_log_file, "a")

    cmd = """
    INSERT INTO {table} VALUES(%(ts)s, %(src)s, %(srcport)s, %(dst)s, %(dstport)s, %(protocol)s, %(len)s)
    """.format(table=table)
    cmd_formats = {"ts": log[0], "src": log[1], "srcport": log[2], "dst": log[3], "dstport": log[4], "protocol": log[5], "len": log[6]}
    curs = self.db_conn.cursor()
    sql_log.write(curs.mogrify(cmd, cmd_formats).decode("utf-8") + "\n\n")
    sql_log.close()
    curs.execute(cmd, cmd_formats)
    self.db_conn.commit()

  def clear_log_table(self, table, clear_ts=None):
    sql_log = open(self.sql_log_file, "a")

    if clear_ts != None:
      cmd = """
      DELETE FROM {table} WHERE timestamp <= %(ts)s
      """.format(table=table)
    else:
      cmd = """
      DELETE FROM {table}
      """.format(table=table)
    cmd_formats = {"ts": clear_ts}
    curs = self.db_conn.cursor()
    sql_log.write(curs.mogrify(cmd, cmd_formats).decode("utf-8") + "\n\n")
    sql_log.close()
    curs.execute(cmd, cmd_formats)
    self.db_conn.commit()

  def clear_summ_table(self, table, clear_ts=None):
    sql_log = open(self.sql_log_file, "a")

    if clear_ts != None:
      cmd = """
      DELETE FROM {table} WHERE min_timestamp <= %(ts)s
      """.format(table=table)
    else:
      cmd = """
      DELETE FROM {table}
      """.format(table=table)
    cmd_formats = {"ts": clear_ts}
    curs = self.db_conn.cursor()
    sql_log.write(curs.mogrify(cmd, cmd_formats).decode("utf-8") + "\n\n")
    sql_log.close()
    curs.execute(cmd, cmd_formats)
    self.db_conn.commit()

  def insert_log_batch(self, table, log_batch):
    # write log_batch to csv
    with open(self.csv_file, "w") as csv_log:
      writer = csv.writer(csv_log, quoting=csv.QUOTE_MINIMAL, quotechar='"', delimiter=",", lineterminator="\r\n")
      for log in log_batch:
        writer.writerow(log)

    # copy csv to table
    sql_log = open(self.sql_log_file, "a")

    cmd = """
    COPY {table} FROM stdin WITH DELIMITER AS ',' csv QUOTE '\"' ESCAPE '\"' NULL ''
    """.format(table=table)
    curs = self.db_conn.cursor()
    sql_log.write(cmd + "\n\n")
    sql_log.close()
    curs.copy_expert(cmd, open(self.csv_file))
    self.db_conn.commit()

  def summarize_table(self, log_table, summ_table):
    sql_log = open(self.sql_log_file, "a")

    cmd = """
    INSERT INTO {summ_table}(min_timestamp, max_timestamp, src, srcport, dst, dstport, protocol, min_length, max_length, avg_length, summ_size)
    SELECT
      min(timestamp),
      max(timestamp),
      src,
      srcport,
      dst,
      dstport,
      protocol,
      min(length),
      max(length),
      avg(length),
      count(*)
    FROM {log_table}
    GROUP BY src, srcport, dst, dstport, protocol
    """.format(log_table=log_table, summ_table=summ_table)
    cmd_formats = {}
    curs = self.db_conn.cursor()
    sql_log.write(curs.mogrify(cmd, cmd_formats).decode("utf-8") + "\n\n")
    sql_log.close()
    curs.execute(cmd, cmd_formats)
    self.db_conn.commit()

  def get_row_count(self, table):
    sql_log = open(self.sql_log_file, "a")

    cmd = """
    SELECT count(*) FROM {table}
    """.format(table=table)
    cmd_formats = {}
    curs = self.db_conn.cursor()
    sql_log.write(curs.mogrify(cmd, cmd_formats).decode("utf-8") + "\n\n")
    sql_log.close()
    curs.execute(cmd, cmd_formats)
    self.db_conn.commit()
    return curs.fetchone()[0]

  def copy_summary_to_csv(self, summ_table, csv_file=None):
    if csv_file == None: csv_file = self.csv_file
    curs = self.db_conn.cursor()

    with open(csv_file, "w") as csv_summ:
      curs.copy_to(csv_summ, summ_table, sep=",",\
        columns=("min_timestamp", "max_timestamp", "src", "srcport", "dst", "dstport", "protocol", "min_length", "max_length", "avg_length", "summ_size"))
    self.db_conn.commit()

  def copy_summary_from_csv(self, summ_table, csv_file=None):
    if csv_file == None: csv_file = self.csv_file
    curs = self.db_conn.cursor()

    with open(csv_file, "r") as csv_summ:
      curs.copy_from(csv_summ, summ_table, sep=",",\
        columns=("min_timestamp", "max_timestamp", "src", "srcport", "dst", "dstport", "protocol", "min_length", "max_length", "avg_length", "summ_size"))
    self.db_conn.commit()

  # return structure: [[src, srcport, dst, dstport, pps, delay], ...]
  def get_summ_pps_info(self, table, timewindow):
    sql_log = open(self.sql_log_file, "a")

    cur_time = time.time()
    cmd = """
    WITH temp AS (SELECT max(max_timestamp) AS max_time FROM {table})
    SELECT src, srcport, dst, dstport, %(cur_time)s-max(max_timestamp), sum(summ_size)/%(timewindow)s
    FROM {table}, temp
    WHERE max_timestamp > max_time-%(timewindow)s
    GROUP BY src, srcport, dst, dstport
    """.format(table=table)
    cmd_formats = {"cur_time":cur_time, "timewindow":timewindow}
    curs = self.db_conn.cursor()
    sql_log.write(curs.mogrify(cmd, cmd_formats).decode("utf-8") + "\n\n")
    sql_log.close()
    curs.execute(cmd, cmd_formats)
    self.db_conn.commit()
    return curs.fetchall()
