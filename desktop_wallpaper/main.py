#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Version : 3.9
# @Time    : 2022-3-3 16:01
# @Author  : Kai Prince
# @File    : main.py
# +-------------------------------------------------------------------


from data_processing import BasicInformationDownload
from downloader import WallpaperDownloader


def main():
    """
    主控制函数，用于调度data_processing、downloader文件的Python程序
    :return:
    """
    # 实例化。调用基础信息生成方法，调用过后避免重复调用
    # basic_information = BasicInformationDownload()
    # basic_information.main()

    # 实例化。调用壁纸下载方法
    wallpaper_downloader = WallpaperDownloader()
    wallpaper_downloader.main()


if __name__ == "__main__":
    main()
