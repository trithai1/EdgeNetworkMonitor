'''
File:     query_process.py
Author:   Quangtri Thai
Contents: Program to run each query on a Postgres table and find the average runtime.
'''

import psycopg2
import time
import sys
import argparse

sql_log_file = "sql_log.SQL"
results_file = "results"
raw_table = "network_log"

# ---General Functions---

def raw_get_end_timestamp(conn):
  global sql_log_file, raw_table
  sql_log = open(sql_log_file, 'a')
  cmd = """SELECT max(timestamp) FROM {raw_table}""".format(raw_table=raw_table)
  curs = conn.cursor()
  curs.execute(cmd)
  sql_log.write(cmd + "\n\n")
  sql_log.close()
  return curs.fetchone()[0]

def raw_get_random_row(conn, end_ts, time_window):
  global sql_log_file, raw_table
  sql_log = open(sql_log_file, 'a')
  start_ts = end_ts - time_window
  cmd = """
  SELECT * FROM (SELECT * FROM {raw_table} WHERE timestamp >= %(start_ts)s) as T
  offset random()*(SELECT count(*) FROM {raw_table} WHERE timestamp >= %(start_ts)s) limit 1
  """.format(raw_table=raw_table)
  curs = conn.cursor()
  cmd_formats = {'start_ts': start_ts}
  curs.execute(cmd, cmd_formats)
  sql_log.write(curs.mogrify(cmd, cmd_formats).decode('utf-8') + "\n\n")
  sql_log.close()
  return curs.fetchone()


# ---Band Join---

def raw_time_band_join_query(conn, end_ts, time_window, join_range):
  global results_file
  results = open(results_file, 'a')
  start_time = time.time()
  joined_rows = raw_run_band_join_query(conn, end_ts, time_window, join_range)
  end_time = time.time()

  print("# of joined rows: " + str(joined_rows))
  results.write("# of joined rows: " + str(joined_rows) + "\n")

  runtime = end_time - start_time
  print("band join query runtime: " + str(runtime) + " seconds")
  results.write("band join query runtime: " + str(runtime) + " seconds\n")
  results.close()
  return runtime

# end_ts is the most recent timestamp currectly in the database.
# time_window is the window of data, in seconds, to preform the band join.
# join_range is the max range, in seconds, one row can be from another if joining.
def raw_run_band_join_query(conn, end_ts, time_window, join_range):
  global sql_log_file, raw_table
  sql_log = open(sql_log_file, 'a')
  start_ts = end_ts - time_window

  cmd = """
  SELECT sum(T.num_rows)
  FROM (
    SELECT L1.src, L1.dst, count(*) as num_rows 
    FROM ({raw_table} AS L1 INNER JOIN {raw_table} AS L2
      ON L1.timestamp-%(join_range)s <= L2.timestamp AND L2.timestamp <= L1.timestamp+%(join_range)s
      AND L1.src != L2.src)
    WHERE %(start_ts)s <= L1.timestamp AND L1.timestamp < %(end_ts)s
      AND %(start_ts)s <= L2.timestamp AND L2.timestamp < %(end_ts)s
    GROUP BY L1.src, L1.dst
  ) AS T;
  """.format(raw_table=raw_table)
  curs = conn.cursor()
  cmd_formats = {'start_ts': start_ts, 'end_ts': end_ts, 'join_range': join_range}
  curs.execute(cmd, cmd_formats)
  sql_log.write(curs.mogrify(cmd, cmd_formats).decode('utf-8') + "\n\n")
  sql_log.close()
  conn.commit()
  return curs.fetchone()[0]


# ---Group By---

def raw_time_group_by_query(conn, end_ts, time_window):
  global results_file
  results = open(results_file, 'a')
  start_time = time.time()
  grouped_rows = raw_run_group_by_query(conn, end_ts, time_window)
  end_time = time.time()

  print("# of grouped rows: " + str(len(grouped_rows)))
  results.write("# of grouped rows: " + str(len(grouped_rows)) + "\n")

  runtime = end_time - start_time
  print("group by query runtime: " + str(runtime) + " seconds")
  results.write("group by query runtime: " + str(runtime) + " seconds\n")
  results.close()
  return runtime

def raw_run_group_by_query(conn, end_ts, time_window):
  global sql_log_file, raw_table
  sql_log = open(sql_log_file, 'a')
  start_ts = end_ts - time_window

  cmd = """
  SELECT src, dst, count(*)
  FROM {raw_table}
  WHERE %(start_ts)s <= timestamp AND timestamp < %(end_ts)s
  GROUP BY src, dst
  """.format(raw_table=raw_table)
  curs = conn.cursor()
  cmd_formats = {'start_ts': start_ts, 'end_ts': end_ts}
  curs.execute(cmd, cmd_formats)
  sql_log.write(curs.mogrify(cmd, cmd_formats).decode('utf-8') + "\n\n")
  sql_log.close()
  conn.commit()
  return curs.fetchall()


# ---Session Duration---

def raw_time_sess_dur_query(conn, end_ts, time_window):
  global results_file
  results = open(results_file, 'a')
  start_time = time.time()
  sessions = raw_run_sess_dur_query(conn, end_ts, time_window)
  end_time = time.time()

  print("# of sessions: " + str(len(sessions)))
  results.write("# of sessions: " + str(len(sessions)) + "\n")

  runtime = end_time - start_time
  print("session duration query runtime: " + str(runtime) + " seconds")
  results.write("session duration query runtime: " + str(runtime) + " seconds\n")
  results.close()
  return runtime

def raw_run_sess_dur_query(conn, end_ts, time_window):
  global sql_log_file, raw_table
  sql_log = open(sql_log_file, 'a')
  start_ts = end_ts - time_window

  cmd = """
  SELECT src, dst, max(timestamp)-min(timestamp)
  FROM {raw_table}
  WHERE %(start_ts)s <= timestamp AND timestamp < %(end_ts)s
  GROUP BY src, dst
  """.format(raw_table=raw_table)
  curs = conn.cursor()
  cmd_formats = {'start_ts': start_ts, 'end_ts': end_ts}
  curs.execute(cmd, cmd_formats)
  sql_log.write(curs.mogrify(cmd, cmd_formats).decode('utf-8') + "\n\n")
  sql_log.close()
  conn.commit()
  return curs.fetchall()


# ---Request/Response Protocol

def raw_time_req_res_query(conn, end_ts, time_window):
  global results_file
  results = open(results_file, 'a')
  start_time = time.time()
  req_res = raw_run_req_res_query(conn, end_ts, time_window)
  end_time = time.time()

  print("# of request/response: " + str(len(req_res)))
  results.write("# of request/response: " + str(len(req_res)) + "\n")

  runtime = end_time - start_time
  print("request/response query runtime: " + str(runtime) + " seconds")
  results.write("request/response query runtime: " + str(runtime) + " seconds\n")
  results.close()
  return runtime

def raw_run_req_res_query(conn, end_ts, time_window):
  global sql_log_file, raw_table
  sql_log = open(sql_log_file, 'a')
  start_ts = end_ts - time_window

  cmd = """
  WITH CTE AS (
    SELECT src, dst
    FROM {raw_table}
    WHERE %(start_ts)s <= timestamp AND timestamp < %(end_ts)s)
  SELECT src, dst
  FROM CTE T1
  WHERE NOT EXISTS (
    SELECT 1
    FROM CTE T2
    WHERE T1.src = T2.dst AND T1.dst = T2.src)
  """.format(raw_table=raw_table)

  curs = conn.cursor()
  cmd_formats = {'start_ts': start_ts, 'end_ts': end_ts}
  curs.execute(cmd, cmd_formats)
  sql_log.write(curs.mogrify(cmd, cmd_formats).decode('utf-8') + "\n\n")
  sql_log.close()
  conn.commit()
  return curs.fetchall()

def main():
  global results_file, sql_log_file
  results = open(results_file, 'w')
  results.close()
  sql_log = open(sql_log_file, 'w')
  sql_log.close()

  parser = argparse.ArgumentParser()
  parser.add_argument("-timewindow", required=True, help="Window size in seconds relative to the largest timestamp in the table")
  parser.add_argument("-join_range", required=True, help="Join range used in the band join query")
  parser.add_argument("-num_runs", required=True, help="Number of runs for each query to get the average time")
  parser.add_argument("-user", required=True, help="User of the database")
  parser.add_argument("-password", required=True, help="User's access password")
  parser.add_argument("-host", required=True, help="Host database is located on")
  parser.add_argument("-database", required=True, help="Name of the database")
  args = parser.parse_args()

  conn = psycopg2.connect(user=args.user,
                          password=args.password,
                          host=args.host,
                          database=args.database)

  end_ts = raw_get_end_timestamp(conn)
  time_window = float(args.timewindow) # seconds
  join_range = float(args.join_range) # second

  print("last timestamp:", end_ts)

  num_runs = abs(int(args.num_runs))
  avg_bandjoin_runtime = 0
  avg_groupby_runtime = 0
  avg_sess_dur_runtime = 0
  avg_req_res_runtime = 0
  for i in range(1, num_runs+1):
    # Band Join: Co-occuring Events
    print("table: " + str(raw_table))
    results = open(results_file, 'a')
    results.write("table: " + str(raw_table) + "\n")
    results.close()
    runtime = raw_time_band_join_query(conn, end_ts, time_window, join_range)
    avg_bandjoin_runtime = avg_bandjoin_runtime * ((i-1)/i) + runtime * (1/i)

    # Group By
    print("\n")
    results = open(results_file, 'a')
    results.write("\n\n")
    results.close()
    runtime = raw_time_group_by_query(conn, end_ts, time_window)
    avg_groupby_runtime = avg_groupby_runtime * ((i-1)/i) + runtime * (1/i)

    # Session Duration
    print("\n")
    results = open(results_file, 'a')
    results.write("\n\n")
    results.close()
    runtime = raw_time_sess_dur_query(conn, end_ts, time_window)
    avg_sess_dur_runtime = avg_sess_dur_runtime * ((i-1)/i) + runtime * (1/i)

    # Request Response Protocol
    print("\n")
    results = open(results_file, 'a')
    results.write("\n\n")
    results.close()
    runtime = raw_time_req_res_query(conn, end_ts, time_window)
    avg_req_res_runtime = avg_req_res_runtime * ((i-1)/i) + runtime * (1/i)

    print("\n---------------------------------------------------------------------------------------------\n")
    results = open(results_file, 'a')
    results.write("\n---------------------------------------------------------------------------------------------\n\n")
    results.close()

  results = open(results_file, 'a')
  print("average bandjoin query runtime overall: " + str(avg_bandjoin_runtime) + " seconds")
  results.write("average bandjoin query runtime overall: " + str(avg_bandjoin_runtime) + " seconds\n")
  print("average groupby query runtime overall: " + str(avg_groupby_runtime) + " seconds")
  results.write("average groupby query runtime overall: " + str(avg_groupby_runtime) + " seconds\n")
  print("average session duration query runtime overall: " + str(avg_sess_dur_runtime) + " seconds")
  results.write("average session duration query runtime overall: " + str(avg_sess_dur_runtime) + " seconds\n")
  print("average request/response query runtime overall: " + str(avg_req_res_runtime) + " seconds")
  results.write("average request/response query runtime overall: " + str(avg_req_res_runtime) + " seconds\n")
  results.close()

if __name__ == '__main__':
  main()
