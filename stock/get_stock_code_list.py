#! -*- coding: utf-8 -*-
import urllib2
import re
import MySQLdb
import os


current_dir = os.path.dirname(os.path.abspath(__file__))


def write_log(message):
    log_dir = os.path.join(current_dir, 'logs')
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_file = os.path.join(log_dir, "get_stock_code_list.log")
    with open(log_file, 'a+') as output:
        output.write(message + "\n")


def get_html(url):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    try:
        request = urllib2.Request(url, headers=headers)
        response = urllib2.urlopen(request)
        html = response.read()
        return html
    except Exception, e:
        print e
        write_log(str(e))
        return False


def get_code_rows(html):
    p = re.compile(r'<tr>.*?<td><a.*?>(.*?)</a></td>.*?<td><a.*?>(.*?)</a></td>.*?</tr>', re.I | re.M | re.S)
    results = re.findall(p, html)
    return results


def get_code_list():
    base_urls = ["http://app.finance.ifeng.com/list/stock.php?t=ha&f=chg_pct&o=desc&p=", "http://app.finance.ifeng.com/list/stock.php?t=sa&f=chg_pct&o=desc&p="]
    conn = MySQLdb.connect('localhost', 'username', 'password', 'astock')
    cur = conn.cursor()
    for base_url in base_urls:
        p = 1
        while p:
            html = get_html(base_url + str(p))
            results = get_code_rows(html)
            if results:
                for result in results:
                    code, name = result
                    if name.startswith('N'):
                        name = name.replace('N', '', 1)
                    if name.startswith('*ST'):
                        name = name.replace('*ST', '', 1)
                    try:
                        cur.execute('insert into code(code, name) values(%s, %s)', (code, name))
                        conn.commit()
                    except Exception, e:
                        print e
                        write_log(str(e))
                p += 1
            else:
                p = False
    conn.close()


if __name__ == "__main__":
    get_code_list()