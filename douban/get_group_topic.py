#!/usr/bin/python
#! -*- coding:utf-8 -*-
import re
import MySQLdb
import time
import random
import requests


f = open('user_agent.txt', 'r')
user_agent_list = f.readlines()
f.close()

main_url = "http://www.douban.com/group/topic/"


def get_html(url):
    try:

        headers = {
            'User-Agent': random.choice(user_agent_list).strip(),
            'Referer': 'http://www.douban.com'
        }

        r = requests.get(url, headers=headers)
        print url, r.status_code
        if r.status_code == 200 or r.status_code == '200':
            return r.content
        else:
            return False
    except Exception, e:
        print e
        return False


def get_topic_info(html):
    p = re.compile(r'<div id="content.*?>.*?<h1>(.*?)</h1>.*?<div class="topic-doc.*?>.*?<span class="from">.*?<a href=.*?/people/(.*?)/">(.*?)</a>(.*?)</span>.*?<span class="color-green.*?">(.*?)</span>.*?<div class="topic-content.*?">(.*?)</div>.*?<div class="title.*?>.*?<a href=".*?/group/(.*?)/\?ref=.*?>(.*?)</a>.*?</div>', re.S | re.M | re.I)
    items = re.findall(p, html)
    return items


def get_conn():
    try:
        conn = MySQLdb.connect(host="localhost", user="username", passwd="password", db="douban", charset="utf8")
        cur = conn.cursor()
    except MySQLdb.Error, e:
        print "MySQL Error: %d, %s" % (e.args[0], e.args[1])
        cur = False
        conn = False
    return conn, cur


start_time = time.time()
conn, cur = get_conn()
topic_id = 0
if cur:
    query_sql = "select topic_id from topic_list order by id desc limit 1"
    cur.execute(query_sql)
    results = cur.fetchall()
    if results:
        topic_id = int(results[0][0]) + 1
        print topic_id
    else:
        topic_id = 1000001

while 1000001 < topic_id < 99999999:
    url = main_url + str(topic_id)
    try:
        topic_html = get_html(url)
        if topic_html:
            item = get_topic_info(topic_html)
            if item:
                items = [x.strip() for x in item[0]]
                items.append(str(topic_id))
                conn, cur = get_conn()
                if cur:
                    cur.execute('insert into topic_list (title, author_id, author_name, signature, post_time, content, group_id, group_name, topic_id) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)', items)
                    conn.commit()
                    cur.close()
                    conn.close()

        time.sleep(random.random() * 10)
    except Exception, e:
        print e

    topic_id += 1

end_time = time.time()
use_time = end_time - start_time
print "Used time: " + str(use_time) + "s"
