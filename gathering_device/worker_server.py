'''
File:     worker_server.py
Author:   Quangtri Thai
Contents: Program to recieve and send messages through sockets as a worker server
'''

import socket, sys
import argparse

import psycopg2

import db_manager

def receive_master(connection, local_dbmanager, remote_dbmanager, summ_table, max_buffer_size):
  while True:
    master_cmd = connection.recv(max_buffer_size)
    if int(master_cmd) == 1:
      local_dbmanager.copy_summary_to_csv(summ_table)
      remote_dbmanager.copy_summary_from_csv(summ_table)
      connection.send(b"1")
      print("Summary sent!")

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("-master", required=True, nargs="+", help="Host address of the master node")
  parser.add_argument("-port", required=True, help="Port to communicate with the master node")
  parser.add_argument("-local_user", required=True, help="User of the local database")
  parser.add_argument("-local_password", required=True, help="User's local access password")
  parser.add_argument("-local_host", required=True, help="Host local database is located on")
  parser.add_argument("-local_database", required=True, help="Name of the local database")
  parser.add_argument("-remote_user", required=True, help="User of the remote database")
  parser.add_argument("-remote_password", required=True, help="User's remote access password")
  parser.add_argument("-remote_host", required=True, help="Host remote database is located on")
  parser.add_argument("-remote_database", required=True, help="Name of the remote database")
  parser.add_argument("-summ_table", required=True, help="Table the stream is summarized to")
  args = parser.parse_args()

  host = args.master
  port = args.port

  soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  print("Socket created!")

  try:
    soc.bind((host,port))
  except:
    print(f"Bind failed. Error: {str(sys.exc_info())}")
    sys.exit(1)

  soc.listen(5)
  print("Socket now listening!")

  max_buffer_size = 5120
  while True:
    connection, address = soc.accept()
    ip, port = str(address[0]), str(address[1])
    print(f"Connection Accepted!")
    try:
      local_dbmanager = db_manager.DBManager(user=args.local_user,
                                         password=args.local_password,
                                         host=args.local_host,
                                         database=args.local_database,
                                         sql_log_file="sql_log_worker_server.SQL",
                                         csv_file="summary.csv")
      remote_dbmanager = db_manager.DBManager(user=args.remote_user,
                                       password=args.remote_password,
                                       host=args.remote_host,
                                       database=args.remote_database,
                                       sql_log_file="sql_log_worker_server.SQL",
                                       csv_file="summary.csv",
                                       clear_log=False)
      summ_table = args.summ_table
      receive_master(connection, local_dbmanager, remote_dbmanager, summ_table, max_buffer_size)
    except:
      connection.shutdown(socket.SHUT_RDWR)
      connection.close()
      print("Master disconnected!")

if __name__ == "__main__":
  main()
