# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re
import MySQLdb
import time
from datetime import date, timedelta
from scrapy import log
from doors_lowes.items import DoorsLowesItem


class DoorsLowesPipeline(object):

	#-- db setting
    db_conn = MySQLdb.connect(
        host    = '127.0.0.1',
        user    = 'root',
        passwd  = '900129lbb',
        db      = 'doors_lowes_keywords',
        charset = 'utf8'
    )
    result_table = ''
    runtime  = ''

    def __init__(self):
        run_time = str(time.strftime("%y%m%d%H%M%s"))

        result_table = run_time + '_result'
        self.result_table = result_table
        log.msg("Scraped data will store in table: %s" % result_table, level=log.INFO)
        
        self.conn = self.db_conn
        self.cursor = self.conn.cursor()

        #-- init result_table
        self.cursor.execute( " CREATE TABLE " + result_table + """(
                id             int              AUTO_INCREMENT,
                site           varchar(25),
                door_type      varchar(255),
                brand          varchar(255),
                name           varchar(255), 
                price          varchar(255), 
                model          varchar(255), 
                link           text, 
                total          varchar(255),
                date           timestamp, 
                PRIMARY KEY (id)
            ) """
        )
        self.conn.commit()

    def process_item(self, item, spider):
        result_table = self.result_table
        
        try:
            self.cursor.execute("""INSERT INTO """ + result_table + """ (site, door_type, brand, name, price, model, link, total, date)
                    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s )""",
                (
                    item['site'],
                    item['door_type'],
                    item['brand'],
                    item['name'],
                    item['price'],
                    item['model'],
                    item['link'],
                    item['total'],
                    str(time.strftime("%Y-%m-%d %H:%M:%S")),
                )
            )
            self.conn.commit()
        except MySQLdb.Error, e:
            log.msg("INSERT DB ERROR %d: %s" % (e.args[0], e.args[1]) , level=log.ERROR)

        return item