import logging
import requests
import datetime
import time
import mysql.connector


INPUT_PAIRS_TUPLE = ('DOGEUSDT', 'COTIUSDT', 'PERLUSDT', 'AUCTIONBTC', 'TROYUSDT', 'REEFUSDT')
#INPUT_PAIRS_TUPLE = ('ENJBTC', 'PPTBTC', 'CELRUSDT', 'CHZUSDT', 'PERLUSDT',
#'TROYUSDT', 'AUDIOUSDT', 'REEFUSDT', 'OGUSDT', 'SFPUSDT', 'AUCTIONBTC')
#INPUT_PAIRS_TUPLE = ('REEFUSDT', 'PERLUSDT')

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO) #, filename='new_screener_test.log'
logger = logging.getLogger(__name__)

#####################DATABASE SCHEME####################

MySQL_TABLES = {}

MySQL_TABLES['screener'] = (
    "CREATE TABLE IF NOT EXISTS `screener` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `date` datetime NOT NULL,"
    "  `pair` varchar(30),"
    "  `time_range` varchar(36),"
    "  `min_prcnt` DECIMAL(10,1) NOT NULL DEFAULT 0.0,"
    "  `common_prcnt` DECIMAL(10,1) NOT NULL DEFAULT 0.0,"
    "  `num_cycles` int(12) NOT NULL,"
    "  `avg_cycle_prcnt` DECIMAL(10,1) NOT NULL DEFAULT 0.0,"
    "  `avg_high_prcnt` DECIMAL(10,1) NOT NULL DEFAULT 0.0,"
    "  `avg_low_prcnt` DECIMAL(10,1) NOT NULL DEFAULT 0.0,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB")

MYSQL_COLUMNS = ('date', 'pair', 'time_range', 'min_prcnt', 'common_prcnt', 'num_cycles',
  'avg_cycle_prcnt', 'avg_high_prcnt', 'avg_low_prcnt')

MYSQL_USERNAME = 'XCram'
MYSQL_PASSWORD = 'mRpsL6385!abrax'
MYSQL_HOST = 'XCram.mysql.pythonanywhere-services.com'
MYSQL_DATABASE = 'XCram$screener_test'

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
    weight_limit = 600 #Max 1200 weight per 1 minute
    res_weight = 0
    DATE_FORMAT = '%Y-%m-%d %H:%M'
    OUTPUT_DATE_FORMAT = '%d-%m-%Y %H:%M'
    def __init__(self, pairs, period, min_prcnt, **add_params):
      self.pair_tuple = pairs
      self.period = period
      self.min_prcnt = min_prcnt
      self.is_sorted_dict = {'sort_by' : None} # 4 -- sort by common prcnt sum
      if 'sort_by' in add_params:
        self.is_sorted_dict['sort_by'] = add_params['sort_by']

    @staticmethod
    def get_all_tickers_dict():
      pair_currency_dict = {}
      req_info = ScreenerBinance.common_public_GET_request('https://api.binance.com/api/v3/exchangeInfo')
      if req_info[0]: #success
        for i in range(len(req_info[1]['symbols'])):
          pair_currency_dict.update({req_info[1]['symbols'][i]['symbol'] : (req_info[1]['symbols'][i]['baseAsset'].upper(),
            req_info[1]['symbols'][i]['quoteAsset'].upper())})
      return pair_currency_dict

    @staticmethod
    def get_pairs_top_list(current_market, top_num):
      out_list = []
      all_pairs = ScreenerBinance.get_all_tickers_dict()
      pairs_list = [x for x in all_pairs if all_pairs[x][1] == current_market]
      return out_list

    @staticmethod
    def get_pair_OHLC_list(pair, **add_data):
      open_list = []
      highs_list = []
      lows_list = []
      close_list = []
      data_valid = False
      q_data = {'symbol': pair, 'interval': '5m'} #'limit': num_candles
      if 'date_from' in add_data:
        start_utime = datetime.datetime.strptime(add_data['date_from'], ScreenerBinance.DATE_FORMAT).timestamp()
        start_time_msec = int(start_utime * 1000)
        q_data['startTime'] = start_time_msec
      if 'date_to' in add_data:
        end_utime = datetime.datetime.strptime(add_data['date_to'], ScreenerBinance.DATE_FORMAT).timestamp()
        end_time_msec = int(end_utime * 1000)
        q_data['endTime'] = end_time_msec
      req_info = ScreenerBinance.common_public_GET_request('https://api.binance.com/api/v3/klines', **q_data)
      if req_info[0]: #success
        open_list = [round(float(x[1]), 8) for x in req_info[1]]
        highs_list = [round(float(x[2]), 8) for x in req_info[1]]
        lows_list = [round(float(x[3]), 8) for x in req_info[1]]
        close_list = [round(float(x[4]), 8) for x in req_info[1]]
        data_valid = True
      return data_valid, open_list, highs_list, lows_list, close_list

    @staticmethod
    def common_public_GET_request(req_url, **add_data):
      if ScreenerBinance.res_weight > ScreenerBinance.weight_limit:
        time.sleep(1)
      success = False
      try:
        response = requests.get(req_url, add_data)
        #print(response.url)
        #print(response.headers)
        if 'x-mbx-used-weight' in response.headers:
          ScreenerBinance.res_weight = int(response.headers['x-mbx-used-weight'])
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

    @staticmethod
    def truncate(n, decimals):
      multiplier = 10 ** decimals
      return int(n * multiplier) / multiplier

    @staticmethod
    def process_vlad_algo(period_hours, min_size, high_list, low_list):
      ### Last move
      last_move_up_list = []
      last_move_down_list = []
      #cur_index = 0
      ###
      max_high = high_list[0]
      min_low = low_list[0]
      up_trend = False
      low_trend = False
      max_step_up = 0
      max_step_down = 0
      for current_high,current_low in zip(high_list, low_list):
        #Step 1
        if current_high > max_high:
          max_high = current_high
        if current_low > min_low:
          min_low = current_low
        ##my additions
        #dev_prcnt_m_high_c_low = (100*max_high/current_low) - 100
        #dev_prcnt_m_low_c_high = (100*min_low/current_high) - 100
        ##my additions end
        #Step 2-3
        if (max_high - current_low) > min_size:
        #if dev_prcnt_m_high_c_low > min_prcnt:
          if not up_trend:
            up_trend = True
            max_step_up = max_high - current_low
          if up_trend:
            if (max_high - current_low) > max_step_up:
              max_step_up = max_high - current_low
        if (min_low - current_high) > min_size:
        #if dev_prcnt_m_low_c_high > min_prcnt:
          if not low_trend:
            low_trend = True
            max_step_down = min_low - current_high
          if low_trend:
            if (min_low - current_high) > max_step_down:
              max_step_down = min_low - current_high
        #Step 2-3
        if up_trend:
          #cur_max_step_up = max_step_up
          #dev_prcnt_m_high_c_low = (100*max_high/current_low) - 100
          #last_move_up_list.append(dev_prcnt_m_high_c_low) #"фиксируем движение max_step_up" (в процентах)
          last_move_up_list.append(max_step_up) #"фиксируем движение max_step_up"
          max_step_up = 0
          up_trend = False
          max_high = current_high
        if low_trend:
          #cur_max_step_down = max_step_down
          last_move_down_list.append(max_step_down) #"фиксируем движение max_step_down"
          #last_move_down_list.append(dev_prcnt_m_low_c_high) #"фиксируем движение max_step_down" (в процентах)
          max_step_down = 0
          low_trend = False
          min_low = current_low
        #Step 4
        if current_high == high_list[-1]: #"если это последняя свеча"
          if up_trend:
            last_move_up_list.uppend(max_step_up) #"фиксируем движение max_step_up"
          if low_trend:
            last_move_down_list.uppend(max_step_down) #"фиксируем движение max_step_down"
      ### my addition, final output
      full_list = last_move_up_list+last_move_down_list
      common_prcnt = sum(full_list)
      avg_common_prcnt = 0.0
      avg_up = 0.0
      avg_down = 0.0
      if len(full_list) > 0:
        avg_common_prcnt = ScreenerBinance.truncate(common_prcnt / len(full_list), 1)
      if len(last_move_up_list) > 0:
        avg_up = ScreenerBinance.truncate(sum(last_move_up_list) / len(last_move_up_list), 1)
      if len(last_move_down_list) > 0:
        avg_down = ScreenerBinance.truncate(sum(last_move_down_list) / len(last_move_down_list), 1)
      output_date_to = datetime.datetime.now().strftime(ScreenerBinance.OUTPUT_DATE_FORMAT)
      output_date_from = (datetime.datetime.now() - datetime.timedelta(hours=period_hours)).strftime(ScreenerBinance.OUTPUT_DATE_FORMAT)
    #   out_list = [datetime.datetime.now(), f'{output_date_from} - {output_date_to}', min_prcnt,
    #     common_prcnt, len(full_list), avg_common_prcnt, avg_up, avg_down]
      return last_move_up_list, last_move_down_list

    @staticmethod
    def process_new_main_algo(min_prcnt, period_hours, open_price, highs_list, lows_list):
      pass

    @staticmethod
    def process_main_algo(min_prcnt, period_hours, open_price, highs_list, lows_list):
      highs_gen = (x for x in highs_list)
      lows_gen = (y for y in lows_list)
      last_price = open_price
      #last_dev_prcnt = None
      #start_checks = 0 #test
      #up_checks = 0 #test
      #down_checks = 0 #test
      #entry_point = None
      last_trend = ''
      prev_dev_prcnt = None
      cycles_list = []
      uptrend_list = []
      downtrend_list = []
      while last_price:
        try:
          next_high = next(highs_gen)
          next_low = next(lows_gen)
          next_high_index = highs_list.index(next_high)
          next_low_index = lows_list.index(next_low)
          prev_high = highs_list[next_high_index-1]
          prev_low = lows_list[next_low_index-1]
          if not last_trend:
            #start_checks += 1 #test
            last_deviation_high_prcnt = (100*next_low/last_price) - 100
            last_deviation_low_prcnt = 100 - (100*next_high/last_price)
            #last_deviation_prcnt = 100 - (100*next_high/last_price)
            if last_deviation_high_prcnt > min_prcnt:
              last_price = next_low
              #entry_point = ['next up', next_high_index]
              last_trend = 'up'
              prev_dev_prcnt = last_deviation_high_prcnt
            elif last_deviation_low_prcnt > min_prcnt:
              last_price = next_high
              #entry_point = ['next down', next_high_index]
              last_trend = 'down'
              prev_dev_prcnt = last_deviation_low_prcnt
              #next_prcnt = -1 * last_deviation_low_prcnt
          elif last_trend == 'up':
            #up_checks += 1 #test
            last_price = next_high
            last_deviation_prcnt = (100*next_high/prev_low) - 100
            price_run = last_deviation_prcnt - prev_dev_prcnt
            if price_run > min_prcnt:
              cycles_list.append(price_run)
              downtrend_list.append(price_run)
              last_trend = 'down'
            else:
              prev_dev_prcnt = last_deviation_prcnt
          elif last_trend == 'down':
            #down_checks += 1 #test
            last_price = next_low
            last_deviation_prcnt = 100 - (100*next_low/prev_high)
            price_run = last_deviation_prcnt - prev_dev_prcnt
            if price_run > min_prcnt:
              cycles_list.append(price_run)
              uptrend_list.append(price_run)
              last_trend = 'up'
            else:
              prev_dev_prcnt = last_deviation_prcnt
        except:
          last_price = None
      #print(entry_point, start_checks, up_checks, down_checks)
      #print(cycles_list)
      #print(uptrend_list)
      #print(downtrend_list)
      common_prcnt = sum(cycles_list)
      cycles_len = len(cycles_list)
      avg_common_prcnt = 0.0
      avg_up = 0.0
      avg_down = 0.0
      if cycles_len > 0:
        avg_common_prcnt = ScreenerBinance.truncate(common_prcnt / cycles_len, 1)
      if len(uptrend_list) > 0:
        avg_up = ScreenerBinance.truncate(sum(uptrend_list) / len(uptrend_list), 1)
      if len(downtrend_list) > 0:
        avg_down = ScreenerBinance.truncate(sum(downtrend_list) / len(downtrend_list), 1)
      output_date_to = datetime.datetime.now().strftime(ScreenerBinance.OUTPUT_DATE_FORMAT)
      output_date_from = (datetime.datetime.now() - datetime.timedelta(hours=period_hours)).strftime(ScreenerBinance.OUTPUT_DATE_FORMAT)
      out_list = [datetime.datetime.now(), f'{output_date_from} - {output_date_to}', min_prcnt,
        common_prcnt, cycles_len, avg_common_prcnt, avg_up, avg_down]
      return out_list

    def start(self):
      #date_to = datetime.datetime.today().strftime(self.DATE_FORMAT)
      output_data = []
      date_from = (datetime.datetime.today() - datetime.timedelta(hours=self.period)).strftime(ScreenerBinance.DATE_FORMAT)
      for pair in self.pair_tuple:
        ohlc_lists = ScreenerBinance.get_pair_OHLC_list(pair, date_from=date_from)
        if ohlc_lists[0]:
          #print(len(ohlc_lists[2]), len(ohlc_lists[3]))
          res_list = ScreenerBinance.process_main_algo(self.min_prcnt, self.period, ohlc_lists[1][0], ohlc_lists[2], ohlc_lists[3])
          res_list.insert(1, pair)
          res_tuple = tuple(res_list)
          output_data.append(res_tuple)
          if self.is_sorted_dict['sort_by']:
            i = self.is_sorted_dict['sort_by']
            output_data = sorted(output_data, key=lambda x: x[i], reverse=True)
      return output_data

if __name__ == '__main__':
  scr_db = ScreenerDB(MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DATABASE)
  scr_db.MySQL_create_tables(MySQL_TABLES)
  #screener = ScreenerBinance('DOGEUSDT', '5m', 1)
  start_time = time.time()
  #all_pairs = ScreenerBinance.get_all_tickers_dict()
  #btc_pairs_list = [x for x in all_pairs if all_pairs[x][1] == 'USDT']
  screener = ScreenerBinance(INPUT_PAIRS_TUPLE, 24, 1, sort_by=4)
  res = screener.start()
  ##date_from = (datetime.datetime.now() - datetime.timedelta(hours=12)).strftime(ScreenerBinance.DATE_FORMAT)
  ##ohlc_lists = ScreenerBinance.get_pair_OHLC_list('DOGEUSDT', date_from=date_from)
  #print(len(ohlc_lists[2]), len(ohlc_lists[3]))
  #l_m_up_list, l_m_down_list = ScreenerBinance.process_vlad_algo(12, 11, [100,90,80,70,60,70,80], [100,90,80,70,60,70,80]) #ohlc_lists[2], ohlc_lists[3]
  #print(l_m_up_list, l_m_down_list)
  ##print(res[:20])
  scr_db.MySQL_add_many_rows_to_table('screener', MYSQL_COLUMNS, res)
  end_time = time.time()
  total_time = end_time - start_time
  logger.info('Time total: %f', total_time)
