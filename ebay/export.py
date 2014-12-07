# -*- coding: utf-8 -*-
import MySQLdb
import xlrd
import xlwt
from xlutils.copy import copy
import time
from numpy import *
from pylab import *
from xlutils.copy import copy
from xlrd import open_workbook
from xlwt import easyxf
import xlwt
from xlwt import *
from datetime import date, timedelta

table_name = '14101923051413774341_result'

class ExportExcel(object):
    def __init__(self):

        self.db_conn = MySQLdb.connect(
            # unix_socket = '/Applications/MAMP/tmp/mysql/mysql.sock',
            # host='localhost',
            # user='root',
            # passwd='900129lbb',
            # db='project_99acres_crawling',
            host='192.168.1.105',
            user='99aceres',
            passwd='go99go',
            db='project_ebay_crawling',
        )


    def get_total(self):
        field = {}
        prices = []
        cursor = self.db_conn.cursor()
        count4offer = 0

        #-- select all bid, buy, offer
        sql = 'SELECT bid, buy, offer FROM %s' % table_name
        cursor.execute(sql)
        records = cursor.fetchall()
        # log.msg("Export Excel", level = log.INFO)
        field['totalNum'] = len(records)
        for bid, buy, offer in records:
            buy = buy.replace(',', '')
            bid = bid.replace(',', '')
            if offer == '1': #--  situation 2
                prices.append(float(buy)/2)
                count4offer += 1
            else:
                if buy == '(None)': #-- situation 1
                    prices.append(float(bid))
                else:
                    if bid == '(None)': #-- situation 1
                        prices.append(float(buy))
                    else: #-- situation 3
                        prices.append((float(buy)+float(bid)) / 2)
        #-- parse the total price
        totalPrice = 0
        for price in prices:
            totalPrice += price
        field['totalPrice'] = round(totalPrice, 2)
        #-- parse the average price
        field['average'] = round(totalPrice / field['totalNum'], 2)
        #-- parse the median price
        if len(prices) % 2 == 1:
            field['median'] = prices[len(prices)/2]
        else:
            field['median'] = prices[len(prices)/2] + prices[len(prices)/2+1]
        field['median'] = round(field['median'], 2)
        #-- parse the percentage of best offer
        field['bestOffer'] = round ((count4offer / float(field['totalNum'])) * 100, 2)
        field['date'] = time.strftime("%Y-%m-%d")

        self.saveWorkSpace(field)


    def saveWorkSpace(self, fields):
        rb = xlrd.open_workbook('Export.xls', formatting_info=True)
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
        wb.save('Export.xls')
        print 'Successfully append to Export.xls'

# In[208]:

export = ExportExcel()
value = {}
# value['date'] = '2014-10-17'
# value['totalNum'] = 10000
# value['totalPrice'] = 1000000
# value['average'] = 124.5
# value['median'] = 231.6
# value['bestOffer'] = 12
# export.saveWorkSpace(value)
export.get_total()

# In[ ]:



