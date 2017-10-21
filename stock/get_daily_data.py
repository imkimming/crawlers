#! -*-coding: utf-8 -*-
import urllib2
import MySQLdb
from decimal import Decimal
import os
import time


base_url = "http://quotes.money.163.com/service/chddata.html?code="
current_dir = os.path.dirname(os.path.abspath(__file__))
cnt = 0


def get_url(code):
    today = time.strftime('%Y%m%d', time.localtime())
    url = "http://quotes.money.163.com/service/chddata.html?code=%s&start=%s" % (code, today)
    return url


def write_log(message, log_type='normal'):
    log_dir = os.path.join(current_dir, 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    if log_type == 'error':
        log_file = "get_daily_data-error.log"
    else:
        log_file = "get_daily_data.log"
    log_path = os.path.join(log_dir, log_file)
    line = "[%s] %s\n" % (time.strftime("%Y-%m-%d %H:%M:%I", time.localtime()), message)
    with open(log_path, 'a+') as output:
        output.write(line)


def get_history_data_csv(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    try:
        r = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(r)
        data = response.read()
        data = data.decode('gbk').encode('utf-8')
        lines = data.strip().split("\n")[1:]
        lines.reverse()
        return lines
    except Exception, e:
        print e, url
        write_log(str(e) + url, 'error')
        return False


def get_history_data():
    try:
        conn = MySQLdb.connect('localhost', 'username', 'password', 'astock')
        cur = conn.cursor()
        cur.execute('select code from code')
        rows = cur.fetchall()
        write_log("Start to get history stock data")
        global cnt
        for row in rows:
            code = row[0]
            today = time.strftime("%Y-%m-%d", time.localtime())
            check_sql = "select code from daily where code='%s' and date='%s'" % (code, today)
            cur.execute(check_sql)
            check_results = cur.fetchall()
            if check_results:
                continue
            else:
                write_log("Start to get history data of stock code: %s" % code)
                if code.startswith('6'):
                    code = '0' + code
                else:
                    code = '1' + code
                lines = get_history_data_csv(get_url(code))
                if lines:
                    line = lines[0]
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        items = line.split(',')
                        for i in range(len(items)):
                            items[i] = items[i].strip()
                            if not items[i] or items[i] == 'None':
                                items[i] = '0'
                        items[1] = items[1].replace("'", '')
                        high_per = (Decimal(items[4]) - Decimal(items[7])) * 100 / Decimal(items[7])
                        low_per = (Decimal(items[5]) - Decimal(items[7])) * 100 / Decimal(items[7])
                        items.append(high_per)
                        items.append(low_per)
                        cur.execute(
                            'insert into daily(date, code, name, close, high, low, open, last_close, chg, chg_per, exchange, vol, amount, total_value, flow_value, deal_cnt, high_per, low_per) values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                            items)
                        conn.commit()
                        cnt += 1
                        message = "Finished adding 1 row history data of stock code: %s" % code
                        write_log(message)
                    except Exception, e:
                        write_log(str(e), 'error')
        cur.close()
        conn.close()
    except Exception, e:
        write_log(str(e), 'error')


if __name__ == "__main__":
    start = time.time()
    get_history_data()
    used_time = time.time() - start
    message = "Added %s records, used time: %ss" % (str(cnt), str(used_time))
    write_log(message)
