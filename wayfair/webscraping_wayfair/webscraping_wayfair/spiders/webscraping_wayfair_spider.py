# -*- coding: utf-8 -*-
import re
import json
from datetime import datetime
from datetime import timedelta
from scrapy import log
from scrapy.spider import BaseSpider
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.exceptions import CloseSpider

from webscraping_wayfair.items import WebscrapingWayfairItem

import random
import struct
import socket
import MySQLdb

import csv


class webscraping_wayfairSpider(BaseSpider):
    # -- scrapy build in attributes
    name = "wayfair"
    allowed_domains = ["wayfair.com", "amazon.com"]


    # headers = {
    # 'Accept'            : 'text/javascript, text/html, application/xml, text/xml, */*',
    #     'Accept-Encoding'   : 'gzip,deflate',
    #     'Accept-Language'   : 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2,zh-CN;q=0.2',
    #     'Content-type'      : 'application/x-www-form-urlencoded; charset=UTF-8',
    #     'Connection'        : 'keep-alive',
    #     'Host'              : 'http://www.99acres.com/',
    #     'User-Agent'        : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36'
    # }
    # db_conn = MySQLdb.connect(
    #     host='127.0.0.1',
    #     user='root',
    #     passwd='900129lbb',
    #     db='doors_homedepot',
    #     charset='utf8'
    # )

    # def get_lins(self):
    #     self.conn = self.db_conn
    #     self.cursor = self.conn.cursor()
    #     self.cursor.execute("""SELECT link, brand, type FROM links""")
    #     rows = self.cursor.fetchall()
    #     links = []
    #     for row in rows:
    #         data = {}
    #         data['link'] = row[0]
    #         data['brand'] = row[1]
    #         data['door_type'] = row[2]
    #         links.append(data)
    #     return links

    wayfairLinks = []
    amazonLinks = []

    def link_parse(self, file):
        links = []
        with open(file, 'rb') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in spamreader:
                if row[0] != '' and row[0] != 'item':
                    self.wayfairLinks.append(row[3])
                    self.amazonLinks.append(row[6])

        if len(self.wayfairLinks) != len(self.amazonLinks):
            self.wayfairLinks = []
            self.amazonLinks = []


    def start_requests(self):
        self.log("Scrape initial", level=log.INFO)
        #get the links need to be scraped
        self.link_parse("falconedge_wayfair.csv")
        # print len(self.wayfairLinks)
        
        #start request amazon
        item_num = 1
        for link in self.amazonLinks:
            yield FormRequest(
                link,
                method="GET",
                meta={
                    'website': "amazon",
                    'item_num': item_num
                },
                callback=self.data_start_parse
            )
            item_num += 1

        #start request wayfair
        item_num = 1
        for link in self.wayfairLinks:
            yield FormRequest(
                link,
                method="GET",
                meta={
                    'website': "wayfair",
                    'item_num': item_num
                },
                callback=self.data_start_parse
            )
            item_num += 1



    def data_start_parse(self, response):
        if response.status == 200:
            self.log("Access item page success.", level=log.INFO)
        else:
            self.log("Item page can't access.", level=log.ERROR)
            raise CloseSpider(reason='Access fail and stop crawler (%s):%s' % (response.status, response.url))

        try:
            meta = response.request.meta
            website = meta['website']
            item_num = meta['item_num']
            print website
            print response.url

            if website == 'wayfair':
                # print 'start wayfair'
                try:
                    description = []
                    tmp = response.xpath("//h1[@class='inlineblock xltext deemphasize']/strong[1]/text()").extract()
                    tmp = "".join(tmp) if tmp else '(None)'
                    if tmp != '(None)':
                        description.append(tmp.strip())
                    tmp = response.xpath("//h1[@class='inlineblock xltext deemphasize']/text()").extract()
                    tmp = "".join(tmp) if tmp else '(None)'
                    if tmp != '(None)':
                        description.append(tmp.strip())
                    final_description = "".join(description)
                except Exception as exp:
                    print 'price parse error'
                    print exp
                    print price
                    final_description = '(None)'

                try:
                    price = []
                    tmp = response.xpath("//span[@data-id='dynamic-sku-price']/text()").extract()
                    tmp = "".join(tmp) if tmp else '(None)'
                    if tmp != '(None)':
                        price.append(tmp.strip())
                    tmp = response.xpath("//span[@data-id='dynamic-sku-price']/sup[1]/text()").extract()
                    tmp = "".join(tmp) if tmp else '(None)'
                    if tmp != '(None)':
                        price.append(tmp.strip())
                    final_price = "".join(price).strip()
                except Exception as exp:
                    print 'price parse error'
                    print exp
                    print price
                    final_price = '(None)'

            else:
                # print 'parse amazon'
                try:
                    final_description = response.xpath("//span[@id='productTitle']/text()").extract()
                    final_description = "".join(final_description) if final_description else '(None)'
                except Exception as exp:
                    print 'price parse error'
                    print exp
                    print price
                    final_description = '(None)'

                try:
                    final_price = response.xpath("//span[@id='priceblock_ourprice']/text()").extract()
                    final_price = "".join(final_price) if final_price else '(None)'
                    if final_price == '(None)':
                        final_price = response.xpath("//span[@id='priceblock_saleprice']/text()").extract()
                        final_price = "".join(final_price) if final_price else '(None)'
                    if final_price != '(None)':
                        final_price = final_price[1:]
                except Exception as exp:
                    print 'price parse error'
                    print exp
                    print price
                    final_price = '(None)'

            item = WebscrapingWayfairItem()

            item['website']     = website
            item['item_num']    = item_num
            item['price']       = final_price
            item['description'] = final_description
            item['link']        = response.url
            

            yield item
        except Exception as exp:
            self.log("%s" % str(exp), level=log.ERROR)
            #save error page
            # file = open(rent_buy_new + '_' + city+'_'+str(current_page)+'.html', 'w')
            # file.write(response.body)
            # file.close()
            #finish saving 