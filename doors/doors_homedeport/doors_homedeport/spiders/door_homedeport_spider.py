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

from doors_homedeport.items import DoorsHomedeportItem

import random
import struct
import socket
import MySQLdb


class webscraping_99acresSpider(BaseSpider):
    # -- scrapy build in attributes
    name = "doors_homedeport"
    allowed_domains = ["homedepot.com"]


    # headers = {
    # 'Accept'            : 'text/javascript, text/html, application/xml, text/xml, */*',
    #     'Accept-Encoding'   : 'gzip,deflate',
    #     'Accept-Language'   : 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2,zh-CN;q=0.2',
    #     'Content-type'      : 'application/x-www-form-urlencoded; charset=UTF-8',
    #     'Connection'        : 'keep-alive',
    #     'Host'              : 'http://www.99acres.com/',
    #     'User-Agent'        : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36'
    # }
    db_conn = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='900129lbb',
        db='doors_homedepot',
        charset='utf8'
    )

    def get_lins(self):
        self.conn = self.db_conn
        self.cursor = self.conn.cursor()
        self.cursor.execute("""SELECT link, brand, type FROM links""")
        rows = self.cursor.fetchall()
        links = []
        for row in rows:
            data = {}
            data['link'] = row[0]
            data['brand'] = row[1]
            data['door_type'] = row[2]
            links.append(data)
        return links

    def start_requests(self):
        self.log("Scrape initial", level=log.INFO)
        #get the links need to be scraped
        links = self.get_lins()
        for link in links:
            # print link['link']
            # print link['brand']
            # print link['door_type']


            yield FormRequest(
                link['link'],
                method="GET",
                meta={
                    'brand': link['brand'],
                    'door_type': link['door_type']
                },
                callback=self.data_start_parse
            )


    def data_start_parse(self, response):
        if response.status == 200:
            self.log("Access homedepot page success.", level=log.INFO)
        else:
            self.log("homedepot page can't access.", level=log.ERROR)
            raise CloseSpider(reason='Access fail and stop crawler (%s):%s' % (response.status, response.url))

        try:
            meta = response.request.meta
            brand = meta['brand']
            door_type = meta['door_type']
            name = response.xpath("//h1[@class='product_title']/text()").extract()[0]
            price = response.xpath("//span[@id='ajaxPrice']/text()").extract()[0].strip()[1:]
            model = response.xpath("//h2[@class='product_details modelNo']/text()").extract()[0].strip()
        #     # self.log("%s - City: %s ,Page: %s ,Status Code: %s" % (rent_buy_new, city, current_page, response.status), level=log.INFO)
        #     # -- save total number

        #             # print result_item_link

            item = DoorsHomedeportItem()

            item['site']        = "homedepot"
            item['door_type']   = door_type
            item['brand']       = brand
            item['name']        = name
            item['price']       = price
            item['model']       = model
            item['link']        = response.url
            

            yield item
        except Exception as exp:
            self.log("%s" % str(exp), level=log.ERROR)
            #save error page
            # file = open(rent_buy_new + '_' + city+'_'+str(current_page)+'.html', 'w')
            # file.write(response.body)
            # file.close()
            #finish saving  