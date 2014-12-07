# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import re
import MySQLdb
import time
from scrapy.exceptions import DropItem
from datetime import date, timedelta
from scrapy import log
from webscraping_adviserinfo.items import WebscrapingAdviserinfoItem

class WebscrapingAdviserinfoPipeline(object):
	# -- db setting
    db_conn = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='900129lbb',
        db='project_adviserinfo_crawling',
        charset='utf8'
    )

    # result_table = 'adviserinfo'
    # result_table = 'test'
    result_table = ''
    output_xls_path = r'output_excel/'
    runtime = ''

    def __init__(self):
        try:
            run_time = str(time.strftime("%y%m%d%H%M%s"))
            result_table = 'WA_result'
            self.result_table = result_table
            result_table = self.result_table
            log.msg("Scraped data will store in table: %s" % result_table, level=log.INFO)

            self.conn = self.db_conn
            self.cursor = self.conn.cursor()

            #-- init result_table
            self.cursor.execute(" CREATE TABLE " + result_table + """(
                    id             int              AUTO_INCREMENT, 
                    datetime       timestamp, 
                    name           varchar(100), 
                    type           tinyint(4), 
                    link           varchar(1000), 
                    zipcode        varchar(6),
                    PRIMARY KEY (id) 
                ) """
            )
            self.conn.commit()
            self.cursor.close()
        except MySQLdb.Error, e:
            log.msg("INITIAL DB ERROR %d: %s" % (e.args[0], e.args[1]), level=log.ERROR)

    def process_item(self, item, spider):
        result_table = self.result_table

        try:
            self.conn = self.db_conn
            self.cursor = self.conn.cursor()
            #-- check if the company is already added
            self.cursor.execute("""SELECT * FROM """ + result_table + """ WHERE link = (%s)""",
                                (
                                    item['link'], 
                                )
            )
            rows = self.cursor.fetchall()
            if len(rows) == 0:
                self.cursor.execute("""INSERT INTO """ + result_table + """ (datetime, name, type, link, zipcode)
                        VALUES ( %s, %s, %s, %s, %s )""",
                                    (
                                        str(time.strftime("%Y-%m-%d %H:%M:%S")),
                                        item['name'],
                                        item['type'],
                                        item['link'],
                                        item['zipcode'],
                                    )
                )
                self.conn.commit()
            self.cursor.close()
        except MySQLdb.Error, e:
            log.msg("INSERT DB ERROR %d: %s" % (e.args[0], e.args[1]), level=log.ERROR)
            # print "INSERT ERROR " + str(exp)

        return item
