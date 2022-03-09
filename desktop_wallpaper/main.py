#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Version : 3.9
# @Time    : 2022-3-3 16:01
# @Author  : Kai Prince
# @File    : main.py
# +-------------------------------------------------------------------
import logging
import os
from datetime import datetime

from data_processing import BasicInformationDownload
from downloader import WallpaperDownloader

LEVELS = [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL]


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
                        filename=f"""logs/log{datetime.now().strftime("%Y-%m-%d")}-主进程.txt""",
                        filemode="a",
                        format='【%(asctime)s】-【%(filename)s】【line:%(lineno)d】【%(levelname)s】: %(''message)s')

    # 实例化。调用基础信息生成方法，调用过后避免重复调用
    basic_information = BasicInformationDownload()
    basic_information.main()

    # 实例化。调用壁纸下载方法
    wallpaper_downloader = WallpaperDownloader()
    wallpaper_downloader.main()


if __name__ == "__main__":
    main()
