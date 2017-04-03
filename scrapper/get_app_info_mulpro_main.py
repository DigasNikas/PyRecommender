
"""
get_app_info_mulpro_main.py


# Example:
    python get_app_info_mulpro_main pkgNamesToCrawl.csv  numOfCores  totalNoLinesinpkgNameCSV
    python get_app_info_mulpro_main crawl_file.csv      16           10000


# By Ehsan Nia
# Aptoide, 2016

v 0.5
change:
    - multiprocessor support

# Database has one table:
# App_data:
# id|appID|MD5|title|packageName|minAge|description|wUrl|icon|icon_hd|screenshots|screenshots_hd|categories|downloadNo|store|crawlDate


"""

import sys
import multiprocessing
import get_app_info_mulpro


if __name__ == '__main__':
    numOfCores = int(sys.argv[2])
    #totalNoLines = 278438     # obtained by  wc -l "filter_whitelist_apps_apt_v1.csv"
    totalNoLines = int(sys.argv[3])

    step = int(totalNoLines / numOfCores) + 1

    jobs = []
    for i in range(numOfCores):   # [0, n-1]
        start_line = (i * step) + 1
        p = multiprocessing.Process(target=get_app_info_mulpro.get_app_info_by_pkgName, args=(sys.argv[1], start_line, step))
        jobs.append(p)
        p.start()