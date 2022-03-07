#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Version : 3.9
# @Time    : 2022-3-3 22:56
# @Author  : Kai Prince
# @File    : downloader.py
# +-------------------------------------------------------------------
from gevent import monkey

# 猴子补丁
monkey.patch_all()

import os
import re
import time
import shutil
from multiprocessing import Process, current_process, cpu_count
from requests.adapters import HTTPAdapter
from gevent.pool import Pool
import pymysql
import requests


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
        self.conn = pymysql.connect(host="localhost", port=3306, user="admin", password="admin", db="wallpaper")
        # 获取当前CPU逻辑核心数
        self.cpu_count = cpu_count()

    def download(self, url_line):
        """
        下载器方法。将每个壁纸下载都当做一个协程进行请求并转存图片
        :param url_line: 从数据库取出的行数据，元组形式，包含(id,title,url)
        :return:
        """
        # 切片的方式取出元组中的数据
        num = url_line[0]
        img_url = url_line[2]
        title = url_line[1]

        # 设置请求重试次数、超时等待时间，并发起请求，得到图片的二进制返回数据
        requests.adapters.DEFAULT_RETRIES = 5
        req_content = requests.get(url=img_url, headers=self.headers, timeout=15).content

        # 将title中的不可用于生成文件夹的字符替换为空，避免生成文件夹报错
        folder = re.sub(r"""[/\\:*"<>|?]""", "", title)

        # 写入读取到的二进制数据----生成图片文件
        with open(f"电脑壁纸/{folder}/{folder}{num}.jpg", "wb") as f:
            f.write(req_content)

        print(f"壁纸{folder}{num}下载成功")

    def gevent_start(self, gevent_list):
        """
        协程定义方法，由进程定义方法进入
        :param gevent_list: 该进程中的协程数量及协程要下载的URL
        :return:
        """
        print(f"----{current_process().name}，进程编号：{current_process().pid}----开启")
        # 定义协程池，数量保持与flag相同
        pool = Pool(6000)
        for url_line in gevent_list:
            pool.map(self.download, (url_line,))
        pool.join()

    def multiprocessing_start(self, url_data_tuple):
        """
        进程定义方法，每个进程默认有一个线程，6000个协程,具体以传入的为准，计算公式为总数/4。len(url_data_tuple) // 4
        :param url_data_tuple: 数据库取出的原始行数据总和元组
        :return:返回子进程列表
        """
        # 协程队列
        gevent_list = list()
        subprocess_list = list()
        # 进程数设置、进程名称计数器
        i = 0
        j = 0
        for url in url_data_tuple:
            i += 1
            # 每次读取出url，将url添加到队列,url中包含id、title、url地址，格式：[(id,title,url),(),]
            gevent_list.append(url)
            # 一定数量的url就启动一个进程并执行
            if i == (len(url_data_tuple) // self.cpu_count):
                j += 1
                p = Process(target=self.gevent_start, name=f"子进程：{j}", args=(gevent_list,))
                subprocess_list.append(p)
                p.start()
                # 重置url队列和计数器
                gevent_list = list()
                i = 0

        return subprocess_list

    def main(self):
        """
        主控制方法，由外部的main.py文件调用
        :return:
        """
        wallpaper_downloader_time_start = time.time()

        # 调用数据表找到壁纸地址的所有行信息，包括id、title、url
        with self.conn.cursor() as cursor:
            cursor.execute("""SELECT * FROM wallpaper.wallpaper_address where title like '%性感%美女%';""")
            url_data_tuple = cursor.fetchall()
            self.conn.close()

        # 按照获取到的壁纸行信息生成文件夹目录，生成规则：电脑壁纸/title名称（去重后）/title名称+id
        if os.path.exists("电脑壁纸"):
            shutil.rmtree("电脑壁纸")
        os.mkdir("电脑壁纸")

        for i in url_data_tuple:
            folder = re.sub(r"""[/\\:*"<>|?]""", "", i[1])
            if os.path.exists(f"电脑壁纸/{folder}"):
                pass
            else:
                os.mkdir(f"电脑壁纸/{folder}")
                print(f"生成目录：电脑壁纸/{folder}--成功")

        # 调用进程生成方法,方法调用结束后返回所有子进程的指向列表
        subprocess_list = self.multiprocessing_start(url_data_tuple)

        # 遍历子进程，设置主进程等待
        for i in subprocess_list:
            i.join()

        wallpaper_downloader_time_end = time.time()
        print(f"总耗时为：{wallpaper_downloader_time_end - wallpaper_downloader_time_start}秒")


if __name__ == "__main__":
    pass
