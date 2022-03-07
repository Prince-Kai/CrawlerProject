#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Version : 3.9
# @Time    : 2022-3-3 22:49
# @Author  : Kai Prince
# @File    : data_processing.py
# +-------------------------------------------------------------------
from gevent import monkey

# 猴子补丁
monkey.patch_all()

from gevent.pool import Pool
import time
import re
import pymysql
import requests


class BasicInformationDownload(object):
    """
    基础信息下载类
    会生成四个数据表：categories（分类）、subtype（子类）、size（尺寸）、wallpaper_package（壁纸包）
    并通过requests库模拟访问目标地址，获取到相关信息写入数据表
    """

    def __init__(self):
        # 定义壁纸地址、伪造请求头、设定请求地址的等待时间
        self.url_origin = "https://desk.zol.com.cn"
        self.url_pc = "https://desk.zol.com.cn/pc/"
        self.url_default = "https://desk.zol.com.cn/2560x1440/"
        self.headers = {
            'content-type': 'application/json',
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
        }
        # 创建类初始化时数据库连接，并生成游标
        self.conn = pymysql.connect(host="localhost", port=3306, user="admin", password="admin", db="wallpaper")
        self.cursor = self.conn.cursor()
        # 初始化创建数据表，初始化后不再调用
        self.create_table()

    def __str__(self):
        print("数据表创建成功")

    def create_table(self):
        """
        数据库表创建，初始化运行一次后不再调用
        :return:
        """
        # 初始化过程生成的四个数据表
        sql_list = [
            """
            CREATE TABLE categories(
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment,
                title VARCHAR (20) NOT NULL, 
                url VARCHAR (100) NOT NULL 
            );
            """,
            """
            CREATE TABLE size( 
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment, 
                title VARCHAR (20) NOT NULL, 
                url VARCHAR (100) NOT NULL 
            );
            """,
            """
            CREATE TABLE subtype(
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment,
                title VARCHAR (20) NOT NULL,
                url VARCHAR (100) NOT NULL,
                parent_id INT UNSIGNED NOT NULL,
                FOREIGN KEY (parent_id) REFERENCES wallpaper.categories(id) 
            );
            """,
            """
            CREATE TABLE wallpaper_package(
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment,
                url VARCHAR (200) NOT NULL
            );
            """,
            """
            CREATE TABLE wallpaper_address(
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment,
                title VARCHAR (100) NOT NULL,
                url VARCHAR (200) NOT NULL
            );
            """,
        ]
        for sql in sql_list:
            self.cursor.execute(sql)
        self.conn.commit()
        print("数据表初始化成功")

    def mysql_conn_del(self):
        """
        数据库连接关闭方法
        :return:
        """
        self.cursor.close()
        self.conn.close()

    def get_categories_size(self):
        """
        模拟网页访问，获取到壁纸分类和尺寸信息
        :return: 分类名称、分类URL、尺寸名称、尺寸URL
        """
        print("----开始下载壁纸基础数据，请稍后！！")

        # 请求ZOL电脑壁纸地址，获取到返回HTML信息
        response_categories = requests.get(url=self.url_pc, headers=self.headers, timeout=15).content.decode("gbk")
        response_replace = response_categories.replace(" ", "").replace("\r", "").replace("\n", "")

        # 通过正则表达式获取到壁纸分类、分类地址、壁纸尺寸、尺寸地址
        self.wallpaper_categories = ["全部"] + re.findall(r"""<ahref="/[a-z]+/">([\w]+)</a>""", response_replace)
        wallpaper_categories_url = ["/pc/"] + re.findall(r"""<ahref="(/[a-z]+/)">[\w]+</a>""", response_replace)
        wallpaper_size = re.findall(r"""<ahref="/[0-9]+x[0-9]+/">([a-z0-9x]+\(*\w*.\w*\)*)</a>""", response_replace)
        wallpaper_size_url = re.findall(r"""<ahref="(/[0-9]+x[0-9]+/)">[a-z0-9x]+\(*\w*.\w*\)*</a>""", response_replace)

        # 拼接分类的完整URL，用于获取分类下的子类信息
        self.url_categories_dict = {self.wallpaper_categories[i]: self.url_origin + wallpaper_categories_url[i] for i in
                                    range(len(self.wallpaper_categories))}
        # 将查询的结果插入到数据表中
        self.insert_categories_size(self.wallpaper_categories, wallpaper_categories_url, wallpaper_size,
                                    wallpaper_size_url)

        print("1----壁纸类型和尺寸查询并插入数据表成功----")

    def get_subtype(self):
        """
        模拟网页访问，获取到子类信息
        :return: 返回子类名称、子类URL
        """
        subtype_title = list()
        subtype_url = list()

        for key, value in self.url_categories_dict.items():
            response_subclass = requests.get(url=value, headers=self.headers, timeout=15).content.decode("gbk")
            response_subclass_replace = response_subclass.replace(" ", "").replace("\r", "").replace("\n", "")

            wallpaper_subtype = re.findall(r"""<ahref="/[a-z]+/[a-z]+/">([\w]+)</a>""", response_subclass_replace)
            wallpaper_subtype_url = re.findall(r"""<ahref="/[a-z]+(/[a-z]+/)">[\w]+</a>""", response_subclass_replace)

            subtype_title.append(wallpaper_subtype)

            subtype_url.append(wallpaper_subtype_url)

        # 将插叙到的数据插入到数据表中
        self.insert_subtype(self.wallpaper_categories, (subtype_title, subtype_url))

        print("2----壁纸子类查询并插入数据表成功----")

    def get_wallpaper_package_address(self, url_input):
        """
        循环调用方法先取出所有页面地址，并根据页面地址取出所有的壁纸包（所有页）
        :param url_input:需要爬取的目标地址
        :return:
        """
        wallpaper_package_url_set = set()

        response_package = requests.get(url=url_input, headers=self.headers, timeout=15).content.decode("gbk")
        response_package_replace = response_package.replace(" ", "").replace("\r", "").replace("\n", "")

        wallpaper_package_url = re.findall(r"""padding"><aclass="pic"href="(/bizhi/[0-9a-z_.]+)""",
                                           response_package_replace)

        wallpaper_package_url_set.update(wallpaper_package_url)

        self.insert_wallpaper_package_address(wallpaper_package_url_set)

    def get_wallpaper_address(self, url_package):
        """
        读取数据表wallpaper_package，获取到全部的壁纸包地址，并进行下述请求，获取到真实的图片下载地址
        :return:
        """
        # 定义一个空的地址字典包，用于存储每个壁纸包中的壁纸下载地址，循环取出每个壁纸包后清空
        wallpaper_all = dict()

        ret = requests.get(url=self.url_origin + url_package, headers=self.headers, timeout=15).content.decode("gbk")
        ret_replace = ret.replace(" ", "").replace("\r", "").replace("\n", "")

        wallpaper_title = re.findall(r"""<aid="titleName"href="/bizhi/[0-9_]+.html">([\w\W]+?)</a>""", ret_replace)
        wallpaper_address = re.findall(r"""<imgsrc[s]*="([/\w.:-]+)"width="\d+"height="\d+">""", ret_replace)

        # 将壁纸下载地址以字典的形式存入，格式：key:壁纸名，value:[壁纸下载地址1,壁纸下载地址2,......]
        wallpaper_all[wallpaper_title[0]] = wallpaper_address

        self.insert_wallpaper_address(wallpaper_all)

    def insert_categories_size(self, wallpaper_categories, wallpaper_categories_url, wallpaper_size,
                               wallpaper_size_url):
        """
        往MySQL分类表、尺寸表中插入数据
        :param wallpaper_categories:壁纸名称
        :param wallpaper_categories_url:壁纸URL
        :param wallpaper_size:壁纸尺寸
        :param wallpaper_size_url:尺寸URL
        :return:
        """
        # 遍历壁纸名称列表，生成SQL语句并插入到分类表
        for i in range(len(wallpaper_categories)):
            sql_categories = """insert into wallpaper.categories values(0,'%s','%s'); """ % (
                wallpaper_categories[i], wallpaper_categories_url[i])
            self.cursor.execute(sql_categories)

        # 遍历壁纸尺寸列表，生成SQL语句并插入到尺寸表
        for j in range(len(wallpaper_size)):
            sql_size = """insert into wallpaper.size values(0,'%s','%s'); """ % (
                wallpaper_size[j], wallpaper_size_url[j])
            self.cursor.execute(sql_size)

        self.conn.commit()

    def insert_subtype(self, wallpaper_categories, subtype_info):
        """
        往子类表中插入数据，依赖于分类信息
        :param wallpaper_categories: 壁纸分类信息
        :param subtype_info: 子类信息集，包含子类名称列表，子类URL列表
        :return:
        """
        subtype_title = subtype_info[0]
        subtype_url = subtype_info[1]

        for i in range(1, len(wallpaper_categories)):
            for j, h in zip(subtype_title[i], subtype_url[i]):
                sql_subtype = """insert into wallpaper.subtype values(0,'%s','%s','%s')""" % (j, h, i + 1)
                self.cursor.execute(sql_subtype)
        self.conn.commit()

    def insert_wallpaper_package_address(self, wallpaper_package_url_set):
        """
        由get_wallpaper_package_address方法调用，往数据表中插入网页返回的壁纸包信息
        :param wallpaper_package_url_set:壁纸包地址集合，已去重
        :return:
        """
        # 遍历壁纸包地址集合，取出所有壁纸包地址，转存如数据表
        for i in wallpaper_package_url_set:
            sql_wallpaper_package = """insert into wallpaper.wallpaper_package values(0,'%s')""" % i
            self.cursor.execute(sql_wallpaper_package)
            print(f"3----壁纸包{i}获取并插入成功----")
        self.conn.commit()

    def insert_wallpaper_address(self, wallpaper_all):
        """
        由get_wallpaper_address方法调用，接收壁纸下载地址字典后，转存入数据表
        :param wallpaper_all:每个壁纸包中的所有壁纸下载地址，格式为字典,key为壁纸包名 value为壁纸地址列表
        :return:
        """
        # 遍历壁纸下载地址字典，取出同一壁纸包下的所有壁纸的地址
        for key, value in wallpaper_all.items():
            for url in value:
                url_re = re.sub(r"\d{3}x\d{2}", "2560x1440", url)
                sql_wallpaper_address = """insert into wallpaper.wallpaper_address values(0,'%s','%s')""" % (
                    key, url_re)
                try:
                    self.cursor.execute(sql_wallpaper_address)
                    print(f"----插入成功{url_re}----")
                except:
                    pass
            self.conn.commit()
            print(f"4----{key}获取壁纸地址成功----")

    def get_wallpaper_address_gevent_start(self):
        # 定义协程池
        pool = Pool(6000)

        # 1.查询数据表wallpaper_package，获取到所有的壁纸包地址，url_package_tuple为元组格式：(("11",),("22",))
        sql_wallpaper_address = """SELECT url from wallpaper.wallpaper_package;"""
        self.cursor.execute(sql_wallpaper_address)
        url_package_tuple = self.cursor.fetchall()

        # 遍历所有壁纸包下载地址，加入到协程池中
        for url_package in url_package_tuple:
            pool.map(self.get_wallpaper_address, url_package)
        pool.join()

    def main(self):
        """
        控制方法，协调调度进行下载、保存
        :return:
        """
        basic_information_time_start = time.time()

        # 调用get_categories_size方法获取分类及尺寸信息，并使用insert_first方法存入数据表categories、size中
        self.get_categories_size()

        # 调用get_subtype方法获取子类信息，并使用insert_second方法存入数据表subtype中
        self.get_subtype()

        # 调用get_wallpaper_package_address方法获取所有壁纸包名称+地址，并使用insert_third方法写入数据表wallpaper_package中
        self.get_wallpaper_package_address(self.url_default)
        # 已知2560x1440的壁纸包有106页
        for i in range(2, 107):
            self.get_wallpaper_package_address(self.url_default + str(i) + ".html")

        # 调用get_wallpaper_address方法获取到所有壁纸真实地址
        self.get_wallpaper_address_gevent_start()

        # 关闭数据库连接方法
        self.mysql_conn_del()

        basic_information_time_end = time.time()
        print(f"总耗时为：{basic_information_time_end - basic_information_time_start}秒")
