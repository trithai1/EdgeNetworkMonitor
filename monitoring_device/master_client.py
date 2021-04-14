'''
File:     master_client.py
Author:   Quangtri Thai
Contents: Program to recieve and send messages through sockets to communicate with multiple worker servers. Displaying information about the data recieved.
'''

import socket, sys
import argparse
import time

from asciimatics.screen import Screen

import db_manager

def main(screen):
  parser = argparse.ArgumentParser()
  parser.add_argument("-pi_hosts", required=True, nargs="+", help="List of host addresses of each raspberry pi")
  parser.add_argument("-port", required=True, help="Port to communicate with each raspberry pi")
  parser.add_argument("-user", required=True, help="User of the database")
  parser.add_argument("-password", required=True, help="User's access password")
  parser.add_argument("-host", required=True, help="Host database is located on")
  parser.add_argument("-database", required=True, help="Name of the database")
  parser.add_argument("-summ_table", required=True, help="Table the stream is summarized to")
  args = parser.parse_args()

  hosts = args.pi_hosts
  port = args.port

  socks = []
  for i in range(len(hosts)):
    try:
      sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      sock.connect((hosts[i],port))
      socks.append(sock)
    except ConnectionRefusedError:
      print(f"Connection failed for {hosts[i]}:{port}")
      sys.exit(1)

  dbmanager = db_manager.DBManager(user=args.user,
                                   password=args.password,
                                   host=args.host,
                                   database=args.database)
  summ_table = args.summ_table
  max_buffer_size=5120
  while True:
    dbmanager.clear_summ_table(summ_table)
    for sock in socks:
      sock.send(b"1")
    for sock in socks:
      sock.recv(max_buffer_size)

    log_window = 1
    pps_info = dbmanager.get_summ_pps_info(summ_table, log_window)
    sorted_pps_info = sorted(pps_info, key=lambda info: (-info[5], info[4]))
    
    screen.clear()
    screen.print_at(f"Window: {log_window} secs", 0, 1)

    col_info = ["src:port", 21, "dst:port", 21, "last occurrence(secs)", 21, "pps", 8] # header follow by max width
    header_str = ""
    for i in range(0, len(col_info), 2):
      header_str += "{0:{1}s} | ".format(col_info[i], col_info[i+1])
    header_str = header_str[:-3]
    screen.print_at(header_str, 0, 2)
    screen.print_at("-"*len(header_str), 0, 3)

    data_offset = 4
    for row in range(len(sorted_pps_info)):
      info = sorted_pps_info[row]
      data_str = ""
      data_str += "{0:{1}s} | ".format(info[0], col_info[1]) if info[1] is None else "{0:{1}s} | ".format(info[0]+":"+str(info[1]), col_info[1])
      data_str += "{0:{1}s} | ".format(info[2], col_info[3]) if info[3] is None else "{0:{1}s} | ".format(info[2]+":"+str(info[3]), col_info[3])
      for i in range(4, len(info)):
        j = (i-2)*2+1
        data = "{:0.2f}".format(info[i]) if isinstance(info[i], int) or isinstance(info[i], float) else str(info[i])
        data_str += "{0:{1}s} | ".format(data, col_info[j]) if i == 4 else "{0:{1}s} | ".format(data, col_info[j])
      data_str = data_str[:-3]
      screen.print_at(data_str, 0, row+data_offset)
    screen.refresh()
    time.sleep(3)

if __name__ == "__main__":
  Screen.wrapper(main)