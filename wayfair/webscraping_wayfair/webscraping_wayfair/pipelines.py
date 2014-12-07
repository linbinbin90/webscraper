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
from webscraping_wayfair.items import WebscrapingWayfairItem
import xlsxwriter

class WebscrapingWayfairPipeline(object):

	#-- db setting
    db_conn = MySQLdb.connect(
        host    = '127.0.0.1',
        user    = 'root',
        passwd  = '900129lbb',
        db      = 'webscraping_wayfair',
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
                website        varchar(25),
                item_num       int,
                description    varchar(255), 
                price          varchar(255),
                link           text, 
                date           timestamp, 
                PRIMARY KEY (id)
            ) """
        )
        self.conn.commit()



    def process_item(self, item, spider):
        result_table = self.result_table
        
        try:
            self.cursor.execute("""INSERT INTO """ + result_table + """ (website, item_num, description, price, link, date)
                    VALUES ( %s, %s, %s, %s, %s, %s )""",
                (
                    item['website'],
                    int(item['item_num']),
                    item['description'],
                    item['price'],
                    item['link'],
                    str(time.strftime("%Y-%m-%d %H:%M:%S")),
                )
            )
            self.conn.commit()
        except MySQLdb.Error, e:
            log.msg("INSERT DB ERROR %d: %s" % (e.args[0], e.args[1]) , level=log.ERROR)

        return item

    def close_spider(self, spider):
    	workbook = xlsxwriter.Workbook('Export.xlsx')
        worksheet = workbook.add_worksheet('Summary')
        worksheet.write('B1', 'Wayfair')
        worksheet.write('E1', 'Amazon')
        worksheet.write('A2', 'Item')
        worksheet.write('B2', 'Description')
        worksheet.write('C2', 'Price')
        worksheet.write('D2', 'Link')
        worksheet.write('E2', 'Description')
        worksheet.write('F2', 'Price')
        worksheet.write('G2', 'Link')
        result_table = self.result_table
        sql = 'SELECT item_num, description, price, link FROM %s where website="wayfair" order by item_num' % result_table
        self.cursor.execute(sql)
        wayfair_records = self.cursor.fetchall()

        row = 2
        new_row = 2
        col = 0
        for item_num, description, price, link in wayfair_records:
            worksheet.write_string(new_row, col, str(item_num))
            worksheet.write_string(new_row, col + 1, description)
            worksheet.write_string(new_row, col + 2, price)
            worksheet.write_string(new_row, col + 3, link)
            new_row += 1

        sql = 'SELECT item_num, description, price, link FROM %s where website="amazon" order by item_num' % result_table
        self.cursor.execute(sql)
        amazon_records = self.cursor.fetchall()
        col += 3
        for item_num, description, price, link in amazon_records:
            worksheet.write_string(row, col + 1, description)
            worksheet.write_string(row, col + 2, price)
            worksheet.write_string(row, col + 3, link)
            row += 1

        workbook.close()
