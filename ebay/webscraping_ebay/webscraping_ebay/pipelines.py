# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import re
import MySQLdb
import time
import xlwt


from numpy import *
from pylab import *
from xlwt import *
from xlutils.copy import copy
import xlrd
from xlwt import easyxf
from scrapy.exceptions import DropItem
from datetime import date, timedelta
from scrapy import log
from webscraping_ebay.items import WebscrapingEbayItem



class WebscrapingEbayPipeline(object):
    # -- db setting
    db_conn = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='900129lbb',
        db='project_ebay_crawling',
        charset='utf8'
    )

    result_table = ''
    output_xls_path = r'output_excel/'
    runtime = ''

    def __init__(self):
        try:
            run_time = str(time.strftime("%y%m%d%H%M%s"))
            result_table = run_time + '_result'
            self.result_table = result_table
            log.msg("Scraped data will store in table: %s" % result_table, level=log.INFO)

            self.conn = self.db_conn
            self.cursor = self.conn.cursor()

            #-- init result_table
            self.cursor.execute(" CREATE TABLE " + result_table + """(
                    id             int              AUTO_INCREMENT, 
                    name           varchar(255), 
                    total          int, 
                    bid            varchar(255), 
                    buy            varchar(255), 
                    offer          varchar(255), 
                    item_link      text, 
                    page_link      text, 
                    size           varchar(255),
                    PRIMARY KEY (id) 
                ) """
            )
            self.conn.commit()
        except MySQLdb.Error, e:
            log.msg("INITIAL DB ERROR %d: %s" % (e.args[0], e.args[1]), level=log.ERROR)

    def process_item(self, item, spider):
        result_table = self.result_table

        try:
            self.cursor.execute("""INSERT INTO """ + result_table + """ (name, total, bid, buy, offer, item_link, page_link, size)
                    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s )""",
                                (
                                    item['name'],
                                    item['total'],
                                    item['bid'],
                                    item['buy'],
                                    item['offer'],
                                    item['item_link'],
                                    item['page_link'],
                                    item['size'],
                                )
            )
            self.conn.commit()
        except MySQLdb.Error, e:
            log.msg("INSERT DB ERROR %d: %s" % (e.args[0], e.args[1]), level=log.ERROR)

        return item

    def close_spider(self, spider):
        self.get_total()

    def get_total(self):
        field = {}
        prices = []
        cursor = self.db_conn.cursor()
        count4offer = 0

        #-- select all bid, buy, offer
        sql = 'SELECT bid, buy, offer FROM %s' % self.result_table
        cursor.execute(sql)
        records = cursor.fetchall()
        # log.msg("Export Excel", level = log.INFO)
        field['totalNum'] = len(records)
        for bid, buy, offer in records:
            buy = buy.replace(',', '')
            bid = bid.replace(',', '')
            if offer == '1':  #--  situation 2
                prices.append(float(buy) / 2)
                count4offer += 1
            else:
                if buy == '(None)':  #-- situation 1
                    prices.append(float(bid))
                else:
                    if bid == '(None)':  #-- situation 1
                        prices.append(float(buy))
                    else:  #-- situation 3
                        prices.append((float(buy) + float(bid)) / 2)
        #-- parse the total price
        totalPrice = 0
        for price in prices:
            totalPrice += price
        field['totalPrice'] = round(totalPrice, 2)
        #-- parse the average price
        field['average'] = round(totalPrice / field['totalNum'], 2)
        #-- parse the median price
        prices.sort()
        if len(prices) % 2 == 1:
            field['median'] = prices[len(prices) / 2]
        else:
            field['median'] = prices[len(prices) / 2] + prices[len(prices) / 2 + 1]
        field['median'] = round(field['median'], 2)
        #-- parse the percentage of best offer
        field['bestOffer'] = round((count4offer / float(field['totalNum'])) * 100, 2)
        field['date'] = time.strftime("%Y-%m-%d")

        self.saveWorkSpace(field)


    def saveWorkSpace(self, fields):
        rb = xlrd.open_workbook(self.output_xls_path + 'Export.xls', formatting_info=True)
        r_sheet = rb.sheet_by_index(0)
        r = r_sheet.nrows
        wb = copy(rb)
        sheet = wb.get_sheet(0)
        sheet.write(r, 0, fields['date'])
        sheet.write(r, 1, fields['totalNum'])
        sheet.write(r, 2, fields['totalPrice'])
        sheet.write(r, 3, fields['average'])
        sheet.write(r, 4, fields['median'])
        sheet.write(r, 5, fields['bestOffer'])
        wb.save(self.output_xls_path + 'Export.xls')
        log.msg('Successfully append to Export.xls', level=log.INFO)
