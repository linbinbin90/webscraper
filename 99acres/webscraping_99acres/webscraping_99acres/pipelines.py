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
from xlrd import open_workbook
from xlwt import easyxf
from scrapy.exceptions import DropItem
from datetime import date, timedelta
from scrapy import log
from webscraping_99acres.items import Webscraping99AcresItem

class Webscraping99AcresPipeline(object):
    #-- db setting
    db_conn = MySQLdb.connect(
        host    = '127.0.0.1',
        user    = 'root',
        passwd  = '900129lbb',
        db      = 'project_99acres_crawling',
        charset = 'utf8'
    )

    cities = ['ahmedabad', 'bangalore', 'chennai', 'hyderabad', 'jaipur', 'kolkata', 'mumbai', 'delhi', 'pune', 'surat']
    types = ['buy', 'new', 'rent']
    template_xls_path = r'excel/template.xls'
    output_xls_path = r'output_excel/'

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
                locality       varchar(255),
                price          FLOAT,
                size           FLOAT,
                date           date, 
                city           varchar(255), 
                rent_buy_new   varchar(255), 
                item_link      text, 
                city_link      text, 
                total          varchar(255), 
                PRIMARY KEY (id)
            ) """
        )
        self.conn.commit()



    def process_item(self, item, spider):

        result_table = self.result_table
        
        try:
            self.cursor.execute("""INSERT INTO """ + result_table + """ (locality, size, price, date, city, rent_buy_new, item_link, city_link, total)
                    VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s )""",
                (
                    item['locality'],
                    item['size'],
                    item['price'],
                    item['date'],
                    item['city'],
                    item['rent_buy_new'],
                    item['item_link'],
                    item['city_link'],
                    item['total'],
                )
            )
            self.conn.commit()
        except MySQLdb.Error, e:
            log.msg("INSERT DB ERROR %d: %s" % (e.args[0], e.args[1]) , level=log.ERROR)

        return item

    def close_spider(self, spider):
        #-- export excel file
        pass
    def close_spider(self, spider):

        self.export_excel(self.result_table)

    def export_excel(self, table_name):
        #-- default excel file
        rb = open_workbook(self.template_xls_path ,formatting_info=True)
        wb = copy(rb)

        #-- open first sheet
        w_sheet = wb.get_sheet(0)
        cursor = self.db_conn.cursor()

        #-- for testing
        # table_name = '14101600571413392239_commonfloor'
        #-- select total
        sql = 'SELECT count(*) FROM %s where (price/size) <= 100 and rent_buy_new = "rent" ' % table_name
        cursor.execute(sql)
        total_rent = cursor.fetchall()
        sql = 'SELECT count(*) FROM %s where rent_buy_new = "buy" ' % table_name
        cursor.execute(sql)
        total_buy = cursor.fetchall()
        sql = 'SELECT count(*) FROM %s where rent_buy_new = "new" ' % table_name
        cursor.execute(sql)
        total_new = cursor.fetchall()
        total = total_rent[0][0] + total_buy[0][0] + total_new[0][0]
        w_sheet.write(35, 1, total)

        index = 2
        Array_price = []
        Array_date = []
        Array_size = []
        for type_name in self.types:
            for city in self.cities:
                if(type_name == 'rent'):
                    #-- select summary price
                    sql = 'SELECT sum(price) FROM %s WHERE city like "%s" and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    total_price = cursor.fetchall()
                    w_sheet.write(index, 2, total_price[0][0])

                    #-- select price and find median
                    sql = 'SELECT price FROM %s WHERE price is not NULL and city like "%s" and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Array_price = cursor.fetchall()
                    if(len(Array_price) != 0):
                        w_sheet.write(index, 3, Array_price[len(Array_price) / 2][0])
                    else:
                        log.msg("Median_price = None",level = log.INFO)

                    #-- select avg price
                    sql = 'SELECT avg(price) FROM %s WHERE price is not NULL and city like "%s" and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Avg_price = cursor.fetchall()
                    w_sheet.write(index, 4, Avg_price[0][0])

                    #-- select date
                    sql = 'SELECT date FROM %s WHERE date != "0000-00-00" and city like "%s" and (price/size) <= 100 and rent_buy_new = "%s" order by date' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Array_date = cursor.fetchall()
                    today = datetime.date.today()
                    if Array_date:
                    #-- find median date
                        w_sheet.write(index, 5, (Array_date[len(Array_date) / 2][0] - today).days)
                        date_count = 0

                    #-- count ( date - today )
                        for date in Array_date:
                            date_count = date_count + (date[0] - today).days
                        w_sheet.write(index, 6, date_count / len(Array_date))
                    else:
                        log.msg("Median_date = None", level = log.INFO)
                        log.msg("Avg_date = None", level = log.INFO)

                    #-- select summary size
                    sql = 'SELECT sum(size) FROM %s WHERE city like "%s" and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    total_size = cursor.fetchall()
                    w_sheet.write(index, 7, total_size[0][0])

                    #-- select size and find median
                    sql = 'SELECT size FROM %s WHERE size is not NULL and city like "%s" and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Array_size = cursor.fetchall()
                    if(len(Array_size) != 0):
                        w_sheet.write(index, 8, Array_size[len(Array_size) / 2][0])
                    else:
                        log.msg("Median_size = None", level = log.INFO)

                    #-- select avg size
                    sql = 'SELECT avg(size) FROM %s WHERE size is not NULL and city like "%s" and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Avg_size = cursor.fetchall()
                    w_sheet.write(index, 9, Avg_size[0][0])

                    #-- select total case of this city and types
                    sql = 'SELECT count(id) FROM %s WHERE city like "%s" and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    total_case = cursor.fetchall()
                    if total_case:
                        w_sheet.write(index, 10, total_case[0][0])
                    else:
                        log.msg("total_case = None", level = log.INFO)
                        w_sheet.write(index, 10, None)

                    index = index + 1

                else:
                    #-- select summary price
                    sql = 'SELECT sum(price) FROM %s WHERE city like "%s" and rent_buy_new = "%s"' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    total_price = cursor.fetchall()
                    w_sheet.write(index, 2, total_price[0][0])

                    #-- select price and find median
                    sql = 'SELECT price FROM %s WHERE price is not NULL and city like "%s" and rent_buy_new = "%s"' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Array_price = cursor.fetchall()
                    if(len(Array_price) != 0):
                        w_sheet.write(index, 3, Array_price[len(Array_price) / 2][0])
                    else:
                        log.msg("Median_price = None",level = log.INFO)

                    #-- select avg price
                    sql = 'SELECT avg(price) FROM %s WHERE price is not NULL and city like "%s" and rent_buy_new = "%s"' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Avg_price = cursor.fetchall()
                    w_sheet.write(index, 4, Avg_price[0][0])

                    #-- select date
                    sql = 'SELECT date FROM %s WHERE date != "0000-00-00" and city like "%s" and rent_buy_new = "%s" order by date' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Array_date = cursor.fetchall()
                    today = datetime.date.today()
                    if Array_date:
                    #-- find median date
                        w_sheet.write(index, 5, (Array_date[len(Array_date) / 2][0] - today).days)
                        date_count = 0

                    #-- count ( date - today )
                        for date in Array_date:
                            date_count = date_count + (date[0] - today).days
                        w_sheet.write(index, 6, date_count / len(Array_date))

                    else:
                        log.msg("Median_date = None", level = log.INFO)
                        log.msg("Avg_date = None", level = log.INFO)

                    #-- select summary size
                    sql = 'SELECT sum(size) FROM %s WHERE city like "%s" and rent_buy_new = "%s"' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    total_size = cursor.fetchall()
                    w_sheet.write(index, 7, total_size[0][0])

                    #-- select size and find median
                    sql = 'SELECT size FROM %s WHERE size is not NULL and city like "%s" and rent_buy_new = "%s"' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Array_size = cursor.fetchall()
                    if(len(Array_size) != 0):
                        w_sheet.write(index, 8, Array_size[len(Array_size) / 2][0])
                    else:
                        log.msg("Median_size = None", level = log.INFO)

                    #-- select avg size
                    sql = 'SELECT avg(size) FROM %s WHERE size is not NULL and city like "%s" and rent_buy_new = "%s"' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    Avg_size = cursor.fetchall()
                    w_sheet.write(index, 9, Avg_size[0][0])

                    #-- select total case of this city and types
                    sql = 'SELECT count(id) FROM %s WHERE city like "%s" and rent_buy_new = "%s"' % (table_name, city ,type_name)
                    cursor.execute(sql)
                    total_case = cursor.fetchall()
                    if total_case:
                        w_sheet.write(index, 10, total_case[0][0])
                    else:
                        log.msg("total_case = None", level = log.INFO)
                        w_sheet.write(index, 10, None)

                    index = index + 1
            #-- For each rent_buy_new
            #-- Sum of price
            w_sheet.write(index, 2, xlwt.Formula("SUM(C%s:C%s)" % (index-9,index) ))
            #-- Sum of size
            w_sheet.write(index, 7, xlwt.Formula("SUM(H%s:H%s)" % (index-9,index) ))
            #-- Sum of case
            w_sheet.write(index, 10, xlwt.Formula("SUM(K%s:K%s)" % (index-9,index) ))
            #-- Median of price
            sql = ""
            if type_name == "rent":
                sql = 'SELECT price FROM %s WHERE price is not NULL and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, type_name)
            else:
                sql = 'SELECT price FROM %s WHERE price is not NULL and rent_buy_new = "%s"' % (table_name, type_name)
            cursor.execute(sql)
            Array_price = cursor.fetchall()
            if Array_price:
                w_sheet.write(index, 3, Array_price[len(Array_price) / 2][0])
            else:
                log.msg( "Median_price = None", level = log.INFO)
            #-- Average of price
            w_sheet.write(index, 4, xlwt.Formula("C%s/K%s" % (index+1,index+1) ))
            #-- Median of price
            sql = ""
            if type_name == "rent":
                sql = 'SELECT size FROM %s WHERE size is not NULL and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, type_name)
            else:    
                sql = 'SELECT size FROM %s WHERE size is not NULL and rent_buy_new = "%s"' % (table_name, type_name)
            cursor.execute(sql)
            Array_size = cursor.fetchall()
            if Array_size:
                w_sheet.write(index, 8, Array_size[len(Array_size) / 2][0])
            else:
                log.msg( "Median_price = None", level = log.INFO)
            #-- Median and average of duration
            sql = ""
            if type_name == "rent":
                sql = 'SELECT date FROM %s WHERE date != "0000-00-00" and rent_buy_new = "%s" and (price/size) <= 100 order by date' % (table_name, type_name)
            else:
                sql = 'SELECT date FROM %s WHERE date != "0000-00-00" and rent_buy_new = "%s" order by date' % (table_name, type_name)
            cursor.execute(sql)
            Array_date = cursor.fetchall()
            today = datetime.date.today()
            if Array_date:
                log.msg( "Median_date = %s" % ((Array_date[len(Array_date) / 2][0] - today).days) , level = log.INFO)
                w_sheet.write(index, 5, (Array_date[len(Array_date) / 2][0] - today).days)
                date_count = 0

                for date in Array_date:
                    date_count = date_count + (date[0] - today).days
                log.msg( "Avg_date = %s" % (date_count / len(Array_date)), level = log.INFO)
                w_sheet.write(index, 6, date_count / len(Array_date))
            else:
                log.msg( "Median_date = None", level = log.INFO)
                log.msg( "Avg_date = None", level = log.INFO)
            #-- Median of size
            sql= ""
            if type_name == "rent":
                sql = 'SELECT size FROM %s WHERE size is not NULL and rent_buy_new = "%s" and (price/size) <= 100' % (table_name, type_name)
            else:
                sql = 'SELECT size FROM %s WHERE size is not NULL and rent_buy_new = "%s"' % (table_name, type_name)
            cursor.execute(sql)
            Array_size = cursor.fetchall()
            if Array_size:
                w_sheet.write(index, 8, Array_size[len(Array_size) / 2][0])
            else:
                log.msg( "Median_price = None", level = log.INFO)
            #-- Average of size
            w_sheet.write(index, 9, xlwt.Formula("H%s/K%s" % (index+1,index+1) ))            
            
            index = index + 1

        wb.save('%s%s.xls'% (self.output_xls_path, table_name))
        self.conn.close()
