# ----------------------------------------- /
# @作者 : Prince Kai
# @时间 : 2022-3-29 11:44:38
# ----------------------------------------- /
import logging
import re
from multiprocessing import cpu_count, Process, current_process

import pymysql
from gevent import monkey
from gevent.pool import Pool

# 猴子补丁
monkey.patch_all()

import time
import requests
from requests.adapters import HTTPAdapter


class BasicInformationDownload(object):
    """
    基础信息下载类
    会生成五个数据表：categories（分类）、subtype（子类）、size（尺寸）、wallpaper_package（壁纸包）、wallpaper_address（壁纸地址）
    并通过requests库模拟访问目标地址，获取到相关信息写入数据表
    """

    def __init__(self):
        logging.debug("【主进程】开始初始化数据表；\n")
        # 定义壁纸地址、伪造请求头、设定请求地址的等待时间
        self.url_origin = "https://desk.zol.com.cn"
        self.url_pc = "https://desk.zol.com.cn/pc/"
        self.size = "2560x1440"
        self.page = 107
        self.url_default = f"https://desk.zol.com.cn/{self.size}/"
        self.headers = {
            'content-type': 'application/json',
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0",
        }
        # 创建类初始化时数据库连接，并生成游标
        self.conn = pymysql.connect(host="192.168.44.131", port=3306, user="root", password="root", db="wallpaper")
        self.cursor = self.conn.cursor()
        # 定义一张壁纸包名称收集列表，用于插入数据库前的去重校验使用
        self.wallpaper_package_title_list = list()
        # 获取当前CPU逻辑核心数
        self.cpu_count = cpu_count()

    def create_table(self):
        """
        数据库表创建，初始化运行一次后不再调用
        :return:
        """
        # 判断是否已存在数据表，存在则删除后重建
        self.cursor.execute("""show tables;""")
        data = self.cursor.fetchall()
        if data:
            for table in data:
                sql = f"""drop table {table[0]};"""
                self.cursor.execute(sql)
                logging.debug(f"【主进程】【数据库】删除数据表{table[0]}; 执行的sql语句：{sql}；\n")
        # 初始化过程生成的5个数据表
        sql_list = [
            """
            CREATE TABLE categories(
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment comment '自增主键',
                title VARCHAR (20) NOT NULL comment '壁纸分类名称', 
                url VARCHAR (100) NOT NULL comment '壁纸分类后缀地址'
            )comment='壁纸分类表，存储壁纸分类名称及对应的后缀地址';
            """,
            """
            CREATE TABLE size( 
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment comment '自增主键', 
                title VARCHAR (20) NOT NULL comment '壁纸尺寸名称', 
                url VARCHAR (100) NOT NULL comment '壁纸尺寸后缀地址'
            )comment='壁纸尺寸表，存储壁纸尺寸名称及对应的后缀地址';
            """,
            """
            CREATE TABLE subtype(
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment comment '自增主键',
                title VARCHAR (20) NOT NULL comment '壁纸子类名称',
                url VARCHAR (100) NOT NULL comment '壁纸子类后缀地址',
                categories_id INT UNSIGNED NOT NULL comment '所属的壁纸分类id'
            )comment='壁纸子类表，存储每个壁纸分类下的子类名称及后缀地址';
            """,
            """
            CREATE TABLE wallpaper_package(
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment comment '自增主键',
                url VARCHAR (200) NOT NULL comment '壁纸包后缀地址'
            )comment='壁纸包表，存储壁纸包后缀地址';
            """,
            """
            CREATE TABLE wallpaper_address(
                id INT UNSIGNED PRIMARY KEY NOT NULL auto_increment comment '自增主键',
                title VARCHAR (100) NOT NULL comment '壁纸图片名称',
                url VARCHAR (200) NOT NULL comment '壁纸图片真实下载地址'
            )comment='壁纸信息表，存储壁纸图片的名称和真实下载地址';
            """,
        ]
        for sql in sql_list:
            self.cursor.execute(sql)

        # 查询已创建的数据表
        self.cursor.execute("""show tables;""")
        tables = self.cursor.fetchall()
        for table in tables:
            logging.debug(f"【主进程】【数据库】生成数据表{table[0]}；\n")

    def mysql_conn_del(self):
        """
        数据库连接关闭方法
        :return:
        """
        self.cursor.close()
        self.conn.close()
        logging.debug("【主进程】【数据库】数据库连接已关闭；\n")

    def get_categories_size(self):
        """
        模拟网页访问，获取到壁纸分类和尺寸信息
        :return: 分类名称、分类URL、尺寸名称、尺寸URL
        """
        logging.debug(f"【主进程】【下载】壁纸分类、壁纸尺寸信息；\n")

        try:
            # 请求ZOL电脑壁纸地址，获取到返回HTML信息
            response_categories = requests.get(url=self.url_pc, headers=self.headers, timeout=15).content.decode("gbk")
            response_replace = response_categories.replace(" ", "").replace("\r", "").replace("\n", "")

            # 通过正则表达式获取到壁纸分类、分类地址、壁纸尺寸、尺寸地址
            wallpaper_categories = ["全部"] + re.findall(r"""<ahref="/[a-z]+/">([\w]+)</a>""", response_replace)
            wallpaper_categories_url = ["/pc/"] + re.findall(r"""<ahref="(/[a-z]+/)">[\w]+</a>""", response_replace)
            wallpaper_size = re.findall(r"""<ahref="/[0-9]+x[0-9]+/">([a-z0-9x]+\(*\w*.\w*\)*)</a>""", response_replace)
            wallpaper_size_url = re.findall(r"""<ahref="(/[0-9]+x[0-9]+/)">[a-z0-9x]+\(*\w*.\w*\)*</a>""", response_replace)

            # 拼接分类的完整URL，用于获取分类下的子类信息
            url_categories_dict = {wallpaper_categories[i]: self.url_origin + wallpaper_categories_url[i] for i in range(len(wallpaper_categories))}

            logging.debug(
                f"【主进程】【网络请求】壁纸分类：{wallpaper_categories}；\n壁纸分类地址：{wallpaper_categories_url}；\n壁纸尺寸{wallpaper_size} ；\n壁纸尺寸地址{wallpaper_size_url}；\n")

            # 将查询的结果插入到数据表中，并启动子类信息查询
            self.insert_categories_size(wallpaper_categories, wallpaper_categories_url, wallpaper_size, wallpaper_size_url)

            logging.debug(f"【主进程】【下载成功】壁纸分类、壁纸尺寸信息；\n")

            self.get_subtype(wallpaper_categories, url_categories_dict)

        except Exception as result:
            logging.debug(f"【主进程】【ERROR】{result}；\n")

    def insert_categories_size(self, wallpaper_categories, wallpaper_categories_url, wallpaper_size, wallpaper_size_url):
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
            sql_categories = """insert into wallpaper.categories values(0,'%s','%s'); """ % (wallpaper_categories[i], wallpaper_categories_url[i])
            logging.debug(f"【主进程】【数据库】壁纸分类插入SQL语句：{sql_categories}；\n")
            self.cursor.execute(sql_categories)

        # 遍历壁纸尺寸列表，生成SQL语句并插入到尺寸表
        for j in range(len(wallpaper_size)):
            sql_size = """insert into wallpaper.size values(0,'%s','%s'); """ % (wallpaper_size[j], wallpaper_size_url[j])
            self.cursor.execute(sql_size)
            logging.debug(f"【主进程】【数据库】壁纸尺寸插入SQL语句：{sql_size}；\n")

        self.conn.commit()
        logging.debug(f"【主进程】【数据库】壁纸分类、尺寸信息提交成功；\n")

    def get_subtype(self, wallpaper_categories, url_categories_dict):
        """
        模拟网页访问，获取到子类信息
        :param wallpaper_categories: 壁纸分类
        :param url_categories_dict: 壁纸分类地址，字典格式
        :return: 返回子类名称、子类URL
        """
        logging.debug(f"【主进程】【下载】壁纸子类信息；\n")

        subtype_title = list()
        subtype_url = list()

        # 遍历壁纸分类地址字典
        for key, value in url_categories_dict.items():
            try:
                response_subclass = requests.get(url=value, headers=self.headers, timeout=15).content.decode("gbk")
                response_subclass_replace = response_subclass.replace(" ", "").replace("\r", "").replace("\n", "")

                wallpaper_subtype = re.findall(r"""<ahref="/[a-z]+/[a-z]+/">([\w]+)</a>""", response_subclass_replace)
                wallpaper_subtype_url = re.findall(r"""<ahref="/[a-z]+(/[a-z]+/)">[\w]+</a>""", response_subclass_replace)

                logging.debug(f"【主进程】【网络请求】壁纸子类：{wallpaper_subtype}；\n壁纸子类地址：{wallpaper_subtype_url}；\n")

                subtype_title.append(wallpaper_subtype)
                subtype_url.append(wallpaper_subtype_url)

            except Exception as result:
                logging.debug(f"【主进程】【ERROR】{result}；\n")

        # 将查询到的数据插入到数据表中
        self.insert_subtype(wallpaper_categories, (subtype_title, subtype_url))
        logging.debug(f"【主进程】【下载成功】壁纸子类信息；\n")

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
                logging.debug(f"【主进程】【数据库】壁纸子类插入SQL语句：{sql_subtype}；\n")
            self.conn.commit()
            logging.debug(f"【主进程】【数据库】壁纸子类信息提交成功；\n")

    def get_wallpaper_package_address_gevent_start(self):
        """
        壁纸包地址获取方法的协程生成方法
        """
        # 定义协程池
        pool = Pool(3000)

        # 将所有页面地址加入到到协程池
        for i in range(1, self.page):
            if i == 1:
                self.get_wallpaper_package_address((self.url_default, 0))
            else:
                pool.map(self.get_wallpaper_package_address, ((self.url_default + str(i) + ".html", i),))
                logging.debug(f"【主进程】【壁纸包协程{i}】创建成功；\n")
        pool.join()

    def get_wallpaper_package_address(self, url_input_i):
        """
        循环调用方法先取出所有页面地址，并根据页面地址取出所有的壁纸包（所有页）
        :param url_input_i:url_input+i的元组，url_input:需要爬取的目标地址；i:协程序号。
        :return:
        """
        url_input = url_input_i[0]
        num = url_input_i[1]

        logging.debug(f"【主进程】【壁纸包协程{num}】壁纸包地址下载协程开启成功；\n")

        wallpaper_package_url_set = set()
        try:
            response_package = requests.get(url=url_input, headers=self.headers, timeout=15).content.decode("gbk")
            response_package_replace = response_package.replace(" ", "").replace("\r", "").replace("\n", "")

            wallpaper_package_url = re.findall(r"""padding"><aclass="pic"href="(/bizhi/[0-9a-z_.]+)""",
                                               response_package_replace)
            wallpaper_package_url_set.update(wallpaper_package_url)

            logging.debug(f"【主进程】【壁纸包协程{num}】【网络请求】壁纸包地址：{wallpaper_package_url}；\n")

            # 将每个协程中的壁纸包地址加入到列表中，用于插入数据表前去重
            self.wallpaper_package_title_list += wallpaper_package_url

            self.insert_wallpaper_package_address(wallpaper_package_url_set, num)

        except Exception as result:
            logging.debug(f"【主进程】【壁纸包协程{num}】【ERROR】捕获到异常：{result}；\n")

    def insert_wallpaper_package_address(self, wallpaper_package_url_set, num):
        """
        由get_wallpaper_package_address方法调用，往数据表中插入网页返回的壁纸包信息
        :param num: 协程编号
        :param wallpaper_package_url_set:壁纸包地址集合，已去重
        :return:
        """
        # 遍历壁纸包地址集合，取出所有壁纸包地址，转存入数据表
        for i in wallpaper_package_url_set:
            if self.wallpaper_package_title_list.count(i) == 1:
                sql_wallpaper_package = """insert into wallpaper.wallpaper_package values(0,'%s')""" % i
                self.cursor.execute(sql_wallpaper_package)
                logging.debug(f"【主进程】【壁纸包协程{num}】【数据库】壁纸包信息插入SQL：{sql_wallpaper_package}；\n")

        self.conn.commit()
        logging.debug(f"【主进程】【壁纸包协程{num}】【数据库】壁纸包信息提交成功；\n")

    def get_wallpaper_address_process_start(self, queue):
        """
        进程定义方法，每个进程默认有一个线程，6000个协程,具体以传入的为准
        :param queue: 消息队列
        :return: 返回子进程列表
        """
        # 查询数据表wallpaper_package，获取到所有的壁纸包地址，url_package_tuple为元组格式：((id1, "url1"),(id2, "url2"))
        sql_wallpaper_address = """SELECT * from wallpaper.wallpaper_package;"""
        self.cursor.execute(sql_wallpaper_address)
        url_package_tuple = self.cursor.fetchall()
        logging.debug(f"【主进程】【数据库】壁纸包信息取出SQL：{sql_wallpaper_address}；\n")
        logging.debug(f"【主进程】【数据库】壁纸包信息：{url_package_tuple}；\n")

        # 协程队列
        gevent_list = list()
        subprocess_list = list()

        # 下载器多进程设置、进程名称计数器
        i = 0
        j = 0
        for idx_url in url_package_tuple:
            i += 1
            # 每次读取出url，将idx和url添加到队列,url中只包含url地址，格式：[(id1, "url1"),(id2, "url2")]
            gevent_list.append(idx_url)
            # 一定数量的url就启动一个进程并执行
            if i == len(url_package_tuple) // (self.cpu_count - 1):
                j += 1
                p = Process(target=self.get_wallpaper_address_gevent_start, name=f"壁纸地址子进程{j}", args=(gevent_list, queue))
                logging.debug(f"【主进程】【壁纸地址子进程{j}】创建成功；\n")
                logging.debug(f"【主进程】【壁纸地址子进程{j}】【协程列表】{gevent_list}；\n")
                subprocess_list.append(p)
                p.start()
                # 重置url队列和计数器
                gevent_list = list()
                i = 0

        if gevent_list:
            p = Process(target=self.get_wallpaper_address_gevent_start, name=f"壁纸地址子进程{self.cpu_count}", args=(gevent_list, queue))
            logging.debug(f"【主进程】【壁纸地址子进程{self.cpu_count}】创建成功；\n")
            logging.debug(f"【主进程】【壁纸地址子进程{self.cpu_count}】【协程列表】{gevent_list}；\n")
            subprocess_list.append(p)
            p.start()

        return subprocess_list

    def get_wallpaper_address_gevent_start(self, gevent_list, queue):
        """
        壁纸地址获取方法的协程生成方法
        :param gevent_list: 协程列表，带有地址和序号
        :param queue: 消息队列
        :return:
        """
        queue.put(f"【{current_process().name}】进程编号：{current_process().pid}开启成功；\n")

        # 定义壁纸下载协程池
        pool = Pool(6000)

        # 遍历所有壁纸包下载地址，加入到协程池中
        for idx, url_package in gevent_list:
            queue.put(f"【{current_process().name}】【壁纸地址协程{idx}】壁纸地址协程开启；\n")
            pool.map(self.get_wallpaper_address, ((url_package, idx, queue),))
        pool.join()

    def get_wallpaper_address(self, url_package_idx_queue):
        """
        读取数据表wallpaper_package，获取到全部的壁纸包地址，并进行下述请求，获取到真实的图片下载地址
        :param url_package_idx_queue: url_package+idx的元组，url_package:壁纸包地址；idx: 协程编号；queue: 消息队列。
        :return:
        """
        url_package = url_package_idx_queue[0]
        idx = url_package_idx_queue[1]
        queue = url_package_idx_queue[2]

        queue.put(f"【{current_process().name}】【壁纸地址协程{idx}】开始下载；\n")

        # 定义一个空的地址字典包，用于存储每张壁纸包中的壁纸下载地址，循环取出每张壁纸包后清空
        wallpaper_all = dict()
        try:
            ret = requests.get(url=self.url_origin + url_package, headers=self.headers, timeout=15).content.decode("gbk")
            ret_replace = ret.replace(" ", "").replace("\r", "").replace("\n", "")

            wallpaper_title = re.findall(r"""<aid="titleName"href="/bizhi/[0-9_]+.html">([\w\W]+?)</a>""", ret_replace)
            wallpaper_address = re.findall(r"""<imgsrc[s]*="([/\w.:-]+)"width="\d+"height="\d+">""", ret_replace)

            queue.put(f"【{current_process().name}】【壁纸地址协程{idx}】【网络请求】壁纸名称：{wallpaper_title}；\n壁纸地址：{wallpaper_address}；\n")

            # 将壁纸下载地址以字典的形式存入，格式：key:壁纸名，value:[壁纸下载地址1,壁纸下载地址2,......]
            wallpaper_all[wallpaper_title[0]] = wallpaper_address

            self.insert_wallpaper_address(wallpaper_all, idx, queue)

        except Exception as result:
            queue.put(f"【{current_process().name}】【壁纸地址协程{idx}】【ERROR】捕获到异常：{result}；\n")

    def insert_wallpaper_address(self, wallpaper_all, idx, queue):
        """
        由get_wallpaper_address方法调用，接收壁纸下载地址字典后，转存入数据表
        :param queue: 消息队列
        :param idx: 协程序号
        :param wallpaper_all:每个壁纸包中的所有壁纸下载地址，格式为字典,key为壁纸包名 value为壁纸地址列表
        :return:
        """
        # 遍历壁纸下载地址字典，取出同一壁纸包下的所有壁纸的地址
        for key, value in wallpaper_all.items():
            for url in value:
                url_re = re.sub(r"\d{3}x\d{2}", self.size, url)
                sql_wallpaper_address = """insert into wallpaper.wallpaper_address values(0,'%s','%s')""" % (key, url_re)
                self.cursor.execute(sql_wallpaper_address)
                queue.put(f"【{current_process().name}】【壁纸地址协程{idx}】【数据库】壁纸地址{url_re}插入成功，SQL语句：{sql_wallpaper_address}；\n")

            self.conn.commit()
            queue.put(f"【{current_process().name}】【壁纸地址协程{idx}】【数据库】壁纸包“{key}”下壁纸地址提交成功；\n")
            queue.put(f"【{current_process().name}】壁纸地址协程{idx}】关闭；\n")

    def main(self, queue):
        """
        控制方法，协调调度进行下载、保存
        """
        # 设置请求重试次数，记录开始时间，
        requests.adapters.DEFAULT_RETRIES = 5
        time_start = time.time()

        # 初始化创建数据表，初始化后不再调用
        self.create_table()

        # 调用get_categories_size方法获取分类及尺寸信息，并使用insert_first方法存入数据表categories、size中
        self.get_categories_size()

        # 调用get_wallpaper_package_address的协程生成方法，使用协程池获取所有壁纸包地址
        logging.debug("【主进程】【下载】开始下载壁纸分类包信息；")
        self.get_wallpaper_package_address_gevent_start()
        logging.debug("【主进程】【下载成功】壁纸分类包信息下载成功；")

        # 调用get_wallpaper_address的进程生成方法，使用协程池获取所有壁纸地址
        subprocess_list = self.get_wallpaper_address_process_start(queue)

        for i in subprocess_list:
            i.join()

        # 关闭数据库连接方法
        self.mysql_conn_del()
        logging.debug("【主进程】基础信息已准备就绪；")

        time_end = time.time()
        logging.debug(f"【耗时】本次基础信息下载总耗时为{time_end - time_start}\n")
