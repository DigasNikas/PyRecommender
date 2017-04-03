"""
get_app_info_mulpro.py

# This script receives an argument for the maximum number of appss to process
# It receives the app data from Aptoide webservice, searching for the package name.
# It connects to the database and saves information on the id's that were not processed yet.
# Example:
python get_store_info.py 20
python get_store_info.py -1  or  python get_store_info.py  => (will process all un-processed records)
python get_store_info.py -r  => resets the default db
python get_store_info.py  crawl.db app.db 100  => input name of crawl list db and app db, and process max 100 records
python get_store_info.py  app_crawl_list.db  app_info.db  100  99  => input name of crawl list db and app db, and process max 99 records, considering records [100 to 199)

# By Ehsan Nia
# Adapted from a code from: Diogo Daniel Soares Ferreira
# Aptoide, 2016

v 0.5
change:
    - multiprocessor support

# Database has two tables:
# App_data:
# id|appID|MD5|title|packageName|minAge|description|wUrl|icon|icon_hd|screenshots|screenshots_hd|categories|store|crawlDate
# Crawl_list
# id|ehs_appNameID|ehs_appCntr|packageName|is_processed|status|crawlDate

Bugs:
# Rarely, some applications are not parsed due to errors on Beautiful Soup parser.
# In the future, change to a more robust database system (MySQL, ...)


"""
from bs4 import BeautifulSoup
import urllib2
import json
import sys, traceback
import sqlite3
import datetime
import time
import os
import multiprocessing



def get_app_info_by_pkgName(cwl_file, start_crawl_line, max_value):
    start_time = time.time()

    url = "http://ws2.aptoide.com/api/7/getAppMeta/package_name="    # using the old ws. since new ws v7-2 does not give categories
    # url = "http://webservices.aptoide.com/webservices/3/getApkInfo/id:"

    name = multiprocessing.current_process().name
    logFile = open("log_"+cwl_file+"_"+str(start_crawl_line), 'a')


    retrieved_items = []
    cntr = 0
    # get the crawl list items
    with open(cwl_file, 'r') as cwl_file_h:
        for line in cwl_file_h.readlines():
            cntr += 1
            if cntr >= start_crawl_line and cntr < start_crawl_line + max_value:
                name_str = line.strip()
                if name_str != '':
                    retrieved_items.append([name_str])
    print(str(name)+" Number of applications imported: {}".format(len(retrieved_items)))

    db_app_name = "db_"+cwl_file+"_"+str(start_crawl_line)
    # create app db file, if not exists.
    if not os.path.isfile(db_app_name):
        db_app = sqlite3.connect(db_app_name)
        create_database(db_app)
        db_app.close()

    db_app = sqlite3.connect(db_app_name)
    c_app = db_app.cursor()



    cntr = 0

    c_app.execute(''' SELECT id FROM app_data ORDER BY id DESC LIMIT 1''')
    cur_id = c_app.fetchone()
    if cur_id != None:
        cur_id = cur_id[0]
    else:
        cur_id = 0

    cntr_failure = 0
    cntr_ok = 0
    downloadNo = 0

    print("Processing list started. from id>={} to id<{} ".format(start_crawl_line, start_crawl_line+max_value))
    print("Number of items in te list: {}".format(len(retrieved_items)))
    logFile.write("Processing list started. from id>={} to id<{} \n".format(start_crawl_line, start_crawl_line+max_value))
    logFile.write("Number of items in te list: {}\n".format(len(retrieved_items)))


    # For all records fetched
    for sid in retrieved_items:
        cntr += 1
        if cntr%10000 == 0:
            print("Current counter: {}".format(cntr))
        if max_value != -1 and cntr > max_value:
                break
        try:
            #print sid
            webpage = urllib2.urlopen(url + str(sid[0]) + "/json")
            soup = BeautifulSoup(webpage, "lxml")
            text = soup.get_text()
            page = json.loads(text)
            if page['info']['status'] == 'OK':
                appID = page['data']['id']
                packageName = page['data']['package']
                store = page['data']['store']['name']
                MD5 = page['data']['file']['md5sum']
                malware = page['data']['file']['malware']['rank']
                title = page['data']['name']
                # print title
                description = page['data']['media']['description']
                wUrl = page['data']['urls']['w']
                """categories = []
                for cat in page['meta']['categories']['standard']:
                    categories.append(cat['name'])
                for cat in page['meta']['categories']['custom']:
                    categories.append(cat['name'])
                minAge = page['meta']['min_age']
                screenshots = []
                screenshots_hd = []
                if 'sshots' in page['media']:
                    for s in page['media']['sshots']:
                        screenshots.append(s)
                if 'sshots_hd' in page['media']:
                    for s in page['media']['sshots_hd']:
                        screenshots_hd.append(s['path'])"""
                if 'pdownloads' in page['data']['stats']:
                    downloadNo = page['data']['stats']['pdownloads']
                icon = page['data']['icon']
                icon_hd = ""
                if 'icon_hd' in page['data']:
                    icon_hd = page['data']['icon_hd']

                cur_id += 1
                now = datetime.datetime.now()
                # id|appID|MD5|malware|title|packageName|minAge|description|wUrl|icon|icon_hd|screenshots|screenshots_hd|categories|downloadNo|store|crawlDate
                c_app.execute(''' INSERT INTO app_data VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ''', (cur_id, appID, MD5, malware, title, packageName, description, wUrl, icon, icon_hd, downloadNo, store, now.isoformat()))
                db_app.commit()
                cntr_ok += 1
            else:
                cntr_failure += 1
                now = datetime.datetime.now()
                print(str(now))
                print ("Importing table data faild! Loaded Page STATUS is NOT OK!")
                print("URL is: "+url + str(sid[0]) + "/json")
                logFile.write("Importing table data faild! Loaded Page STATUS is NOT OK\n!")
                logFile.write("URL is: "+url + str(sid[0]) + "/json\n")
        except:
            print("Error during parsing")
            print("Counter: {}".format(cntr))
            print("app: {}".format(sid))
            logFile.write("Error during parsing\n")
            logFile.write("Counter: {} \t".format(cntr))
            logFile.write("app: {} \n".format(sid))
            traceback.print_exc(file=sys.stdout)

    finish_time = time.time()
    print("........................")
    print( "Processing list finished")
    print( "Elapsed time: {} (s)".format(finish_time - start_time))
    print( "Number of items in te list: {}".format(len(retrieved_items)))
    print( "OK: {}".format(cntr_ok))
    print( "FAILURE: {}".format(cntr_failure))
    print( "........................")

    logFile.write( "........................\n")
    logFile.write( "Processing list finished\n")
    logFile.write( "Elapsed time: {} (s)\n".format(finish_time - start_time))
    logFile.write( "Number of items in te list: {}\n".format(len(retrieved_items)))
    logFile.write( "OK: {}\n".format(cntr_ok))
    logFile.write( "FAILURE: {}\n".format(cntr_failure))
    logFile.write( "........................\n")

    logFile.close()
    db_app.close()


def create_database(db_app):
    c = db_app.cursor()
    c.execute(''' DROP TABLE IF EXISTS app_data ''')
    c.execute(''' CREATE TABLE app_data ( `id` BIGINT, `appID` INTEGER, `MD5` TEXT, `Malware` TEXT, `title` text, `packageName` TEXT, `description` text, `wUrl` text, `icons` TEXT, `icon_hd` TEXT, `downloadNo` text, `store` text, `crawlDate` TEXT ) ''')
    db_app.commit()
    print("app data table created/re-initialized.")
#    logFile.write("app data table created/re-initialized.\n")






