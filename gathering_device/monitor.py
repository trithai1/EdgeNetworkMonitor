import time
import argparse
import csv

import pyshark
from asciimatics.screen import Screen

import db_manager
# import data_processor # no need to pre-process data anymore


# runs and coordinates the monitoring of the network
# table_timewindow is in units of minutes and summ_timewindow is in seconds
class Monitor():
  def __init__(self, interface, table_timewindow, summ_timewindow, log_table, summ_table, user, password, host, database, display_rate, display_size):
    self.db_manager = db_manager.DBManager(user, password, host, database)
    # self.data_processor = data_processor.DataProcessor()
    self.table_timewindow = table_timewindow
    self.summ_timewindow = summ_timewindow
    self.capture = pyshark.LiveCapture(interface=interface)
    self.log_table = log_table
    self.summ_table = summ_table

    self.monitor_file = "monitor.csv"
    open(self.monitor_file, "w").close()
    monitor_log = open(self.monitor_file, "a")
    writer = csv.writer(monitor_log, quoting=csv.QUOTE_MINIMAL, quotechar='"', delimiter=",", lineterminator="\r\n")
    writer.writerow(["summ row count", "src:port", "dst:port", "pps", "delay"])
    monitor_log.close()

    self.check_ts = time.time()
    self.display_rate = display_rate # seconds
    self.delay_count = 0
    self.avg_delay = 0
    self.total_delay = 0
    self.display_size = display_size
    self.skip_count = 0

  def run(self, screen):
    log_batch = []
    count = 0
    for packet in self.capture.sniff_continuously():
      # extract needed data from packet
      log = []
      log.append(self.get_sniff_ts(packet))
      log.append(self.get_src(packet))
      log.append(self.get_src_port(packet))
      log.append(self.get_dst(packet))
      log.append(self.get_dst_port(packet))
      log.append(self.get_protocol(packet))
      log.append(self.get_length(packet))
      # log = self.data_processor.process_log(log)
      if log[1] == None or log[3] == None:
        self.skip_count += 1
        continue
      log_batch.append(log)

      # push to db in batchs of summ_timewindow
      start_ts = float(log_batch[0][0])
      end_ts = float(log_batch[len(log_batch)-1][0])
      if self.summ_timewindow == None:
        # use db_manager to insert log to network_log
        # clear network_log of past data based on table_timewindow
        self.db_manager.insert_log_batch(self.log_table, log_batch)
        self.log_monitor(screen, log_batch, self.log_table)
        clear_ts = end_ts-self.table_timewindow*60
        self.db_manager.clear_log_table(self.log_table, clear_ts)
        log_batch = []
      elif end_ts-start_ts >= self.summ_timewindow:
        # use data_processor to process log_batch
        # use db_manager to write log_batch to csv, then copy csv to network_log_temp
        # use db_manager to summarize network_log_batch to network_log_summary
        # clear network_log_summary of past data based on table_timewindow
        # clear network_log_batch of all data
        self.db_manager.insert_log_batch(self.log_table, log_batch)
        self.db_manager.summarize_table(self.log_table, self.summ_table)
        self.log_monitor(screen, log_batch, self.log_table, self.summ_table)
        clear_ts = 0
        self.db_manager.clear_log_table(self.log_table)
        clear_ts = end_ts-self.table_timewindow*60
        self.db_manager.clear_summ_table(self.summ_table, clear_ts)
        log_batch = []

  # Functions to get info from the packet, often returning None if not found

  def layer_exist(self, packet, layer):
    return layer in [layer.layer_name for layer in packet.layers]

  def get_sniff_ts(self, packet):
    return packet.sniff_timestamp

  def get_src(self, packet):
    if self.layer_exist(packet, "ip"):
      return packet.ip.src
    elif self.layer_exist(packet, "eth"):
      return packet.eth.src
    elif self.layer_exist(packet, "wlan"):
      if "sa" in dir(packet.wlan):
        return packet.wlan.sa
      elif "ta" in dir(packet.wlan):
        return packet.wlan.ta + " (TA)"
    return None

  def get_dst(self, packet):
    if self.layer_exist(packet, "ip"):
      return packet.ip.dst
    elif self.layer_exist(packet, "eth"):
      return packet.eth.dst
    elif self.layer_exist(packet, "wlan"):
      if "da" in dir(packet.wlan):
        return packet.wlan.da
      elif "ra" in dir(packet.wlan):
        return packet.wlan.ra + " (RA)"
    return None

  def get_src_port(self, packet):
    if self.layer_exist(packet, "tcp"):
      return packet.tcp.srcport
    elif self.layer_exist(packet, "udp"):
      return packet.udp.srcport
    return None

  def get_dst_port(self, packet):
    if self.layer_exist(packet, "tcp"):
      return packet.tcp.dstport
    elif self.layer_exist(packet, "udp"):
      return packet.udp.dstport
    return None 

  def get_protocol(self, packet):
    return packet.highest_layer

  def get_length(self, packet):
    return packet.length

  # Functions to monitor the program

  def log_monitor(self, screen, log_batch, log_table, summ_table=None):
    cur_ts = time.time()
    latest_ts = float(log_batch[len(log_batch)-1][0])
    delay_ts = cur_ts-latest_ts

    self.total_delay = delay_ts
    self.delay_count += 1
    if (cur_ts - self.check_ts) >= self.display_rate:
      self.avg_delay = self.total_delay/self.delay_count

      # get summ pps info and print it
      # organized by highest pps, lowest delay. only look at 60sec time window.
      pps_info = self.db_manager.get_summ_pps_info(self.summ_table, self.display_size)
      sorted_pps_info = sorted(pps_info, key=lambda info: (-info[5], info[4]))

      screen.clear()
      screen.print_at(f"Avg Delay: {self.avg_delay:0.2f} secs", 0, 0)
      screen.print_at(f"Window: {self.display_size} secs", 0, 1)
      screen.print_at(f"Total Skipped Packets: {self.skip_count}", 0, 2)

      col_info = ["src:port", 26, "dst:port", 26, "last occurrence(secs)", 21, "pps", 8] # header follow by max width
      header_str = ""
      for i in range(0, len(col_info), 2):
        header_str += "{0:{1}s} | ".format(col_info[i], col_info[i+1])
      header_str = header_str[:-3]
      screen.print_at(header_str, 0, 3)
      screen.print_at("-"*len(header_str), 0, 4)

      data_offset = 5
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

      self.check_ts = time.time()
      self.total_delay = 0
      self.delay_count = 0

def main():
  parser = argparse.ArgumentParser()
  parser.add_argument("-interface", required=True, help="Interface to sniff")
  parser.add_argument("-table_timewindow", required=True, help="Time window size of the table in mintues. Controls how much of the network is kept relative to the current timestamp")
  parser.add_argument("-summ_timewindow", required=True, help="Time window size of the summarization in seconds. Controls the the batch size in which the data is summarized")
  parser.add_argument("-log_table", required=True, help="Temporary table where the stream would be stored before being summarized")
  parser.add_argument("-summ_table", required=True, help="Table where the stream is summarized to")
  parser.add_argument("-user", required=True, help="User of the database")
  parser.add_argument("-password", required=True, help="User's access password")
  parser.add_argument("-host", required=True, help="Host database is located on")
  parser.add_argument("-database", required=True, help="Name of the database")
  parser.add_argument("-displayrate", required=True, help="Update rate, in seconds, at which the stream is displayed to the user")
  parser.add_argument("-displaysize", required=True, help="Size, in seconds, of the stream displayed to the user relative to the current timestamp")
  args = parser.parse_args()

  # monitor = Monitor(args.interface, 20, None, "network_log", None, args.user, args.password, args.host, args.database)
  monitor = Monitor(args.interface, float(args.table_timewindow), float(args.summ_timewindow), args.log_table, args.summ_table, args.user, args.password, args.host, args.database, float(args.displayrate), float(args.displaysize))
  Screen.wrapper(monitor.run)

if __name__ == '__main__':
  main()