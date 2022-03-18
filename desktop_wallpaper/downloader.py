#!/usr/bin/env python_version3.9.7
# -*- coding: utf-8 -*-
# @Time    : 2022-3-9 10:46
# @Author  : Kai Prince
# @File    : downloader.py
# @Version : 1.0
# +-------------------------------------------------------------------
import logging
import logging.handlers
import os
import re
import shutil
from multiprocessing import Process, current_process, cpu_count, freeze_support, Queue

import pymysql
from gevent import monkey
from gevent.pool import Pool

# 猴子补丁
monkey.patch_all()

import time
from datetime import datetime
from requests.adapters import HTTPAdapter
import requests

LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]


class WallpaperDownloader(object):
    """
    壁纸下载器类，定义有初始化方法、下载方法、进程定义方法、协程定义方法、主控制方法
    """

    def __init__(self):
        # 定义请求头信息
        self.headers = {
            "content-type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
        }
        # 连接数据库，读取出所有的文件下载地址
        self.conn = pymysql.connect(host="localhost", port=3306, user="root", password="root", db="wallpaper")
        # 获取当前CPU逻辑核心数
        self.cpu_count = cpu_count()

    def set_folder(self):
        """
        壁纸文件夹生成方法
        :return:返回查询数据库得到的数据
        """
        # 调用数据表找到壁纸地址的所有行信息，包括id、title、url
        with self.conn.cursor() as cursor:
            cursor.execute("""SELECT * FROM wallpaper.wallpaper_address;""")
            url_data_tuple = cursor.fetchall()
            self.conn.close()
            logging.debug(f"【主进程】【数据库】获取全部壁纸地址成功；\n")

        # 按照获取到的壁纸行信息生成文件夹目录，生成规则：电脑壁纸/title名称（去重后）/title名称+id
        if os.path.exists("电脑壁纸"):
            shutil.rmtree("电脑壁纸")
            logging.debug(f"【主进程】【文件】删除文件夹成功；\n")
        os.mkdir("电脑壁纸")
        logging.debug(f"【主进程】【文件】生成文件夹成功；\n")

        # 遍历查询数据库得到的壁纸地址行信息，创建各分类的文件夹
        for i in url_data_tuple:
            folder = re.sub(r"""[/\\:*"<>|?]""", "", i[1])
            if os.path.exists(f"电脑壁纸/{folder}"):
                pass
            else:
                os.mkdir(f"电脑壁纸/{folder}")
                logging.debug(f"【主进程】【文件】生成文件目录成功，目录地址：电脑壁纸/{folder}；\n")

        return url_data_tuple

    def multiprocessing_start(self, url_data_tuple, queue):
        """
        进程定义方法，每个进程默认有一个线程，6000个协程,具体以传入的为准，计算公式为总数/4。len(url_data_tuple) // 4
        :param url_data_tuple: 数据库取出的原始行数据总和元组、
        :param queue:消息队列
        :return:返回子进程列表
        """
        # 协程队列
        gevent_list = list()
        subprocess_list = list()

        # 下载器多进程设置、进程名称计数器
        i = 0
        j = 0
        for url in url_data_tuple:
            i += 1
            # 每次读取出url，将url添加到队列,url中包含id、title、url地址，格式：[(id,title,url),(),]
            gevent_list.append(url)
            # 一定数量的url就启动一个进程并执行
            if i == (len(url_data_tuple) // (self.cpu_count - 1)):
                j += 1
                p = Process(target=self.gevent_start, name=f"子进程{j}", args=(gevent_list, queue))
                logging.debug(f"【主进程】【子进程{j}】创建成功；\n")
                subprocess_list.append(p)
                p.start()
                # 重置url队列和计数器
                gevent_list = list()
                i = 0

        return subprocess_list

    def gevent_start(self, gevent_list, queue):
        """
        协程定义方法，由进程定义方法进入
        :param gevent_list: 该进程中的协程数量及协程要下载的URL
        :param queue:消息队列
        :return:
        """
        queue.put(f"【{current_process().name}】进程编号：{current_process().pid}开启成功；\n")

        # 定义协程池，数量保持与flag相同
        pool = Pool(6000)
        for url_line in gevent_list:
            url_queue_tuple = (url_line, queue)
            queue.put(f"【{current_process().name}】【协程{url_line[0]}】壁纸下载协程{url_line}创建成功；\n")
            pool.map(self.download, (url_queue_tuple,))
        pool.join()

    def download(self, url_queue_tuple):
        """
        下载器方法。将每个壁纸下载都当做一个协程进行请求并转存图片
        :param url_queue_tuple:url_line+queue组成的元组。queue:消息队列。url_line: 从数据库取出的行数据，元组形式，包含(id,title,url)
        :return:
        """
        queue = url_queue_tuple[1]

        # 切片的方式取出元组中的数据
        num = url_queue_tuple[0][0]
        title = url_queue_tuple[0][1]
        img_url = url_queue_tuple[0][2]

        queue.put(f"【{current_process().name}】【协程{num}】壁纸下载协程开启成功；\n")

        try:
            # 设置超时等待时间，并发起请求，得到图片的二进制返回数据
            req_content = requests.get(url=img_url, headers=self.headers, timeout=15).content

            # 将title中的不可用于生成文件夹的字符替换为空，避免生成文件夹报错
            folder = re.sub(r"""[/\\:*"<>|?]""", "", title)

            # 写入读取到的二进制数据----生成图片文件
            with open(f"电脑壁纸/{folder}/{folder}{num}.jpg", "wb") as f:
                f.write(req_content)

            queue.put(f"【{current_process().name}】【协程{num}】电脑壁纸/{folder}/{folder}{num}.jpg 保存成功；\n")

        except Exception as result:
            queue.put(f"【{current_process().name}】【协程{num}】【ERROR】捕获到异常：{result}；\n")

        queue.put(f"【{current_process().name}】【协程{num}】壁纸下载协程关闭成功；\n")

    def set_listen_process(self, queue):
        """
        监听进程配置日志对象方法
        :param queue: 传输消息队列对象
        :return:
        """
        # 设置日志等级和输出日志格式
        logging.basicConfig(level=LEVELS[0],
                            filename=f"""logs/log{datetime.now().strftime("%Y-%m-%d")}-子进程.txt""",
                            filemode="a",
                            format='【%(asctime)s】-【%(filename)s】【line:%(lineno)d】【%(levelname)s】: %(''message)s')

        logger = logging.getLogger(__name__)

        # 死循环接收消息队列中的其他子进程日志信息，直至主进程消亡后死亡
        while True:
            res = queue.get()
            logger.debug(f"【进程】{res}")

    def main(self):
        """
        主控制方法，由外部的main.py文件调用
        :return:
        """
        # 创建消息队列
        queue = Queue()

        # windows不支持fork函数，为解决打包成exe后无限制开启子进程的bug
        freeze_support()

        # 设置请求重试次数，记录开始时间，
        requests.adapters.DEFAULT_RETRIES = 5
        time_start = time.time()

        # 调用壁纸文件夹生成方法
        url_data_tuple = self.set_folder()

        # 建立监听子进程，并设置为守护进程，随主进程一同消亡
        listen_process = Process(target=self.set_listen_process, name="子进程0", args=(queue,))
        listen_process.daemon = True
        listen_process.start()
        logging.debug(f"【主进程】【{listen_process.name}】进程编号：{listen_process.pid}开启成功；\n")

        # 调用进程生成方法,方法调用结束后返回所有子进程的指向列表
        subprocess_list = self.multiprocessing_start(url_data_tuple, queue)

        # 遍历子进程，设置主进程等待
        for i in subprocess_list:
            i.join()

        time_end = time.time()
        logging.debug(f"【主进程】【{listen_process.name}】进程编号：{listen_process.pid}关闭成功；\n")
        time.sleep(0.1)
        logging.debug(f"【耗时】下载壁纸总耗时{time_end - time_start}\n")
