# -*- coding:utf-8 -*-
"""
作者：苎夏星染
日期：2022年11月30日
"""
import re
import socket
import sqlite3

import requests
import pypinyin
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from loguru import logger
import json
import time

logger.add("log.txt", rotation="10MB", encoding="UTF-8", enqueue=True, retention="10 days")


def server_client(new_socket, client_addr, accept_data_):
    print(client_addr)
    while True:
        temp_data = new_socket.recv(1024)
        accept_data_ += temp_data
        if len(temp_data) < 1024:
            break
    try:
        accept_data = json.loads(accept_data_.decode("utf-8"))
    except Exception():
        return
    if accept_data[0] == "获取信息":
        print(accept_data)
        data = crawler(accept_data[1])
        new_socket.send(json.dumps(data).encode("utf-8"))
    elif accept_data[0] == "保存信息":
        print(accept_data[0])
        save_data(accept_data[1])
        new_socket.send("true".encode("utf-8"))
    elif accept_data[0] == "获取图表":
        data = get_data(accept_data[1])
        new_socket.send(json.dumps(data).encode("utf-8"))
    else:
        data = ["我也不知道给你回点什么", "找点其他的东西看看吧"]
        new_socket.send(json.dumps(data).encode("utf-8"))


def crawler(x: str):
    ct = "".join(pypinyin.lazy_pinyin(x, style=pypinyin.Style.FIRST_LETTER))
    data = []
    for num in range(1, 3):
        if x == "哈尔滨":
            url = f"https://hrb.lianjia.com/ershoufang/pg{num}"
        else:
            url = f"https://{ct}.lianjia.com/ershoufang/pg{num}"
        r = requests.get(url)
        headers = {
            'Connection': 'close'
        }
        r.headers = headers
        r.encoding = "UTF-8"
        if r.status_code != 200:
            logger.error(f"Failed to crawl the site content,error is '{Exception()}'")
            raise Exception()
        html_doc = r.text
        soup = BeautifulSoup(html_doc, "html.parser")
        ul_nodes = soup.find_all("div", class_="info clear")
        for ul_node in ul_nodes:
            link = ul_node.find_all("a")
            link1 = ul_node.find_all("div", class_="houseInfo")
            link2 = ul_node.find("div", class_="totalPrice totalPrice2")
            link3 = ul_node.find("div", class_="unitPrice")
            li = [('标题', link[0].get_text()),
                  ('地址', link[1].get_text() + link[2].get_text()),
                  ('详细介绍', link1[0].get_text()), ('总价', link2.get_text()), ('单价', link3.get_text()),
                  ('网址', link[0]["href"])]
            data.append(dict(li))
    logger.info("Successfully crawled the website content")
    return data


def save_data(datas):
    con = sqlite3.connect("cities.db")
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS cities(城市 TEXT,标题 TEXT,地址 TEXT,详细介绍 TEXT,"
        "总价 TEXT,单价 TEXT,网址 TEXT,UNIQUE(城市, 标题))")
    for data in datas:
        cur.execute("INSERT OR REPLACE INTO cities VALUES('{}','{}','{}','{}','{}','{}','{}')".format(data["城市"],
                                                                                                      data["标题"],
                                                                                                      data["地址"],
                                                                                                      data["详细介绍"],
                                                                                                      data["总价"],
                                                                                                      data["单价"],
                                                                                                      data["网址"]))
        con.commit()
    cur.close()
    con.close()


def update_data():
    city_list = ["北京", "上海", "天津", "重庆", "广州", "深圳", "苏州", "成都", "武汉", "南京", "杭州", "沈阳", "青岛",
                 "大连", "宁波", "西安", "长春", "厦门", "哈尔滨", "济南", "福州", "长沙", "合肥", "郑州", "南昌",
                 "石家庄", "太原", "昆明", "兰州", "呼和浩特", "乌鲁木齐"]
    for city in city_list:
        time.sleep(10)
        data = crawler(city)
        print(f"更新数据成功:{city}")
        for date_ in data:
            date_["城市"] = city
        save_data(data)


def get_data(num):
    city_list = ["北京", "上海", "天津", "重庆", "广州", "深圳", "苏州", "成都", "武汉", "南京", "杭州", "沈阳", "青岛",
                 "大连", "宁波", "西安", "长春", "厦门", "哈尔滨", "济南", "福州", "长沙", "合肥", "郑州", "南昌",
                 "石家庄", "太原", "昆明", "兰州", "呼和浩特", "乌鲁木齐"]
    city_data = {}
    for city in city_list:
        city_data[city] = 0
    con = sqlite3.connect("cities.db")
    cur = con.cursor()
    cur.execute("SELECT 城市,单价 FROM cities")
    datas = cur.fetchall()
    for index, data in enumerate(datas):
        unit_price = int("".join(re.findall(r"\d+", data[1])))
        if unit_price <= num:
            city_data[data[0]] += 1
    cur.close()
    con.close()
    return city_data


if __name__ == '__main__':
    scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
    scheduler.add_job(update_data, "cron", day_of_week="3", minute="56")
    scheduler.start()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("", 1000))
    server_socket.listen(128)
    new, addr = server_socket.accept()

    while True:
        temp_data_ = new.recv(1)
        if not temp_data_:
            print("连接已断开，等待连接")
            new, addr = server_socket.accept()
            time.sleep(10)
            continue
        server_client(new, addr, temp_data_)
