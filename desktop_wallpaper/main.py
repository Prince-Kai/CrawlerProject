#!/usr/bin/env python_version3.9.12
# -*- coding : utf-8 -*-
# @Time      : 2022/3/29 11:44:38
# @Author    : Prince Kai
# @File      : main.py
# @Version   : 1.1
# +-------------------------------------------------------------------
import logging
from multiprocessing import Process, Queue, freeze_support
import os
from datetime import datetime

from data_processing import BasicInformationDownload
from downloader import WallpaperDownloader

LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]


def set_listen_process(queue):
    """
    监听进程配置日志对象方法
    :return:
    """
    # 设置日志等级和输出日志格式
    logging.basicConfig(level=LEVELS[0],
                        filename=f"""logs/log{datetime.now().strftime("%Y-%m-%d")}.txt""",
                        filemode="a",
                        format='【%(asctime)s】-【%(filename)s】【line:%(lineno)d】【%(levelname)s】: %(''message)s')

    logger = logging.getLogger(__name__)

    # 死循环接收消息队列中的其他子进程日志信息，直至主进程消亡后死亡
    while True:
        res = queue.get()
        logger.debug(f"【子进程】{res}")


def main():
    """
    主控制函数，用于调度data_processing、downloader文件的Python程序
    :return:
    """
    # 生成日志文件夹
    if not os.path.exists("logs"):
        os.mkdir("logs")

    # 设置主进程的日志等级和输出日志格式
    logging.basicConfig(level=LEVELS[0],
                        filename=f"""logs/log{datetime.now().strftime("%Y-%m-%d")}.txt""",
                        filemode="a",
                        format='【%(asctime)s】-【%(filename)s】【line:%(lineno)d】【%(levelname)s】: %(''message)s')

    # 创建消息队列
    queue = Queue()

    # windows不支持fork函数，为解决打包成exe后无限制开启子进程的bug
    freeze_support()

    # 建立监听子进程，并设置为守护进程，随主进程一同消亡
    listen_process = Process(target=set_listen_process, name="监听进程", args=(queue,))
    listen_process.daemon = True
    listen_process.start()
    logging.debug(f"【主进程】【{listen_process.name}】进程编号：{listen_process.pid}开启成功；\n")

    # 实例化。调用基础信息生成方法，调用过后避免重复调用
    basic_information = BasicInformationDownload()
    basic_information.main(queue)

    # 实例化。调用壁纸下载方法
    wallpaper_downloader = WallpaperDownloader()
    wallpaper_downloader.main(queue)

    logging.debug(f"【主进程】【{listen_process.name}】进程编号：{listen_process.pid}关闭成功；\n")


if __name__ == "__main__":
    main()
