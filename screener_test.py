import logging
import requests
import datetime
import mysql.connector

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO) #, filename='screener_test.log'
logger = logging.getLogger(__name__)

#####################DATABASE SCHEME####################

MySQL_TABLES = {}

MySQL_TABLES['screener'] = (
    "CREATE TABLE IF NOT EXISTS `screener` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `date` datetime NOT NULL,"
    "  `pair` varchar(30),"
    "  `time_range` varchar(30),"
    "  `min_prcnt` DECIMAL(3,1) NOT NULL DEFAULT 0.0,"
    "  `common_prcnt` DECIMAL(3,1) NOT NULL DEFAULT 0.0,"
    "  `num_cycles` int(12) NOT NULL,"
    "  `avg_cycle_prcnt` DECIMAL(3,1) NOT NULL DEFAULT 0.0,"
    "  `avg_high_prcnt` DECIMAL(3,1) NOT NULL DEFAULT 0.0,"
    "  `avg_low_prcnt` DECIMAL(3,1) NOT NULL DEFAULT 0.0,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")

MYSQL_COLUMNS = ('date', 'pair', 'time_range', 'min_prcnt', 'common_prcnt', 'num_cycles',
  'avg_cycle_prcnt', 'avg_high_prcnt', 'avg_low_prcnt')

MYSQL_USERNAME = 'username'
MYSQL_PASSWORD = 'password'
MYSQL_HOST = 'hostname'
MYSQL_DATABASE = 'db_name'

class ScreenerDB:
    def __init__(self, username, passwd, hostname, db_name):
      self.username = username
      self.passwd = passwd
      self.hostname = hostname
      self.db_name = db_name

    def MySQL_create_tables(self, tables_dict):
      try:
        db = mysql.connector.connect(user=self.username, password=self.passwd, host=self.hostname, database=self.db_name)
        cursor = db.cursor()
        for table_name in tables_dict:
          table_description = tables_dict[table_name]
          try:
            cursor.execute(table_description)
            #logger.info('MySQL created table: %s', table_name)
          except mysql.connector.Error as e:
            logger.error('MySQL database error: %s', e)
        cursor.close()
        db.close()
      except mysql.connector.Error as e:
        logger.error('MySQL database connection error: %s', e)

    def MySQL_add_new_row_to_table(self, table, columns, values):
      try:
        db = mysql.connector.connect(user=self.username, password=self.passwd, host=self.hostname, database=self.db_name)
        cursor = db.cursor()
        sql = "INSERT INTO {0} ({1}) VALUES ({2})".format(table, ', '.join('{}'.format(column) for column in columns), ', '.join('{}'.format('%s') for x in range(len(columns))))
        try:
          cursor.execute(sql, values)
          db.commit()
        except mysql.connector.Error as e:
          db.rollback()
          logger.error('MySQL database error: %s', e)
        cursor.close()
        db.close()
      except mysql.connector.Error as e:
        logger.error('MySQL database connection error: %s', e)

    def MySQL_add_many_rows_to_table(self, table, columns, data_list):
      try:
        db = mysql.connector.connect(user=self.username, password=self.passwd, host=self.hostname, database=self.db_name)
        cursor = db.cursor()
        sql = "INSERT INTO {0} ({1}) VALUES ({2})".format(table, ', '.join('{}'.format(column) for column in columns), ', '.join('{}'.format('%s') for x in range(len(columns))))
        try:
          cursor.executemany(sql, data_list)
          db.commit()
        except mysql.connector.Error as e:
          db.rollback()
          logger.error('MySQL database error: %s', e)
        cursor.close()
        db.close()
      except mysql.connector.Error as e:
        logger.error('MySQL database connection error: %s', e)

class ScreenerBinance:
    screeners_total = 0
    DATE_FORMAT = '%Y-%m-%d %H:%M'
    def __init__(self, pair, period, min_prcnt):
      ScreenerBinance.screeners_total += 1
      self.pair = pair
      self.period = period
      self.min_prcnt = min_prcnt

    def get_pair_OHLC_list(self, **add_data):
      open_list = []
      highs_list = []
      lows_list = []
      close_list = []
      data_valid = False
      q_data = {'symbol': self.pair, 'interval': self.period} #'limit': num_candles
      if 'date_from' in add_data:
        start_utime = datetime.datetime.strptime(add_data['date_from'], ScreenerBinance.DATE_FORMAT).timestamp()
        start_time_msec = int(start_utime * 1000)
        q_data['startTime'] = start_time_msec
      if 'date_to' in add_data:
        end_utime = datetime.datetime.strptime(add_data['date_to'], ScreenerBinance.DATE_FORMAT).timestamp()
        end_time_msec = int(end_utime * 1000)
        q_data['endTime'] = end_time_msec
      req_info = self.common_public_GET_request('https://api.binance.com/api/v3/klines', **q_data)
      if req_info[0]: #success
        open_list = [round(float(x[1]), 8) for x in req_info[1]]
        highs_list = [round(float(x[2]), 8) for x in req_info[1]]
        lows_list = [round(float(x[3]), 8) for x in req_info[1]]
        close_list = [round(float(x[4]), 8) for x in req_info[1]]
        data_valid = True
      return data_valid, open_list, highs_list, lows_list, close_list

    def common_public_GET_request(self, req_url, **add_data):
      success = False
      try:
        response = requests.get(req_url, add_data)
        #print(response.url)
        if response.status_code == requests.codes.ok:
          resp_result = response.json()
          #print(list(resp_result.keys()))
          if resp_result:
            success = True
          return success, resp_result
        else:
          #resp_result = response.text()
          logger.error('Binance public get request error code %d', response.status_code)
          return success, response.status_code
      except Exception as e:
        logger.error('Binance connection error: %s', e)
        return success, e

    def truncate(self, n, decimals):
      multiplier = 10 ** decimals
      return int(n * multiplier) / multiplier

    def calculate(self):
      #date_to = datetime.datetime.today().strftime(self.DATE_FORMAT)
      date_from = (datetime.datetime.today() - datetime.timedelta(hours=24)).strftime(self.DATE_FORMAT)
      ohlc_lists = self.get_pair_OHLC_list(date_from=date_from)
      #print(len(ohlc_lists[2]), len(ohlc_lists[3]))
      highs_gen = (x for x in ohlc_lists[2])
      lows_gen = (y for y in ohlc_lists[3])
      last_price = ohlc_lists[1][0]
      cycles_list = []
      output_data = []
      while last_price:
        try:
          next_high = next(highs_gen)
          next_low = next(lows_gen)
          last_deviation_high_prcnt = (100*next_high/last_price) - 100
          last_deviation_low_prcnt = 100 - (100*next_high/last_price)
          if last_deviation_high_prcnt > self.min_prcnt:
            last_price = next_high
            cycles_list.append(last_deviation_high_prcnt)
          elif last_deviation_low_prcnt > self.min_prcnt:
            last_price = next_low
            next_prcnt = -1 * last_deviation_low_prcnt
            cycles_list.append(next_prcnt)
        except:
          last_price = None
      min_prcnt = self.truncate(min(cycles_list), 1)
      common_prcnt = sum(cycles_list)
      cycles_len = len(cycles_list)
      avg_common_prcnt = self.truncate(common_prcnt / cycles_len, 1)
      up_list = [up for up in cycles_list if up > 0]
      down_list = [down for down in cycles_list if down < 0]
      avg_up = self.truncate(sum(up_list) / len(up_list), 1)
      avg_down = self.truncate(sum(down_list) / len(down_list), 1)
      output_data = (datetime.datetime.now(), self.pair, 'Last 24 hours', min_prcnt,
        common_prcnt, cycles_len, avg_common_prcnt, avg_up, avg_down)
      return output_data

if __name__ == '__main__':
  scr_db = ScreenerDB(MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
  scr_db.MySQL_create_tables(MySQL_TABLES)
  screener = ScreenerBinance('DOGEUSDT', '5m', 1)
  res = screener.calculate()
  scr_db.MySQL_add_new_row_to_table('screener', MYSQL_COLUMNS, res)
