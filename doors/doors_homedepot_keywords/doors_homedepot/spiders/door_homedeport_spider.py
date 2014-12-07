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

from doors_homedepot.items import DoorsHomedepotItem

import random
import struct
import socket
import MySQLdb


class webscraping_homedepotSpider(BaseSpider):
    # -- scrapy build in attributes
    name = "doors_homedepot"
    allowed_domains = ["homedepot.com"]
    base_url = "http://www.homedepot.com"


    # headers = {
    # 'Accept'            : 'text/javascript, text/html, application/xml, text/xml, */*',
    #     'Accept-Encoding'   : 'gzip,deflate',
    #     'Accept-Language'   : 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2,zh-CN;q=0.2',
    #     'Content-type'      : 'application/x-www-form-urlencoded; charset=UTF-8',
    #     'Connection'        : 'keep-alive',
    #     'Host'              : 'http://www.99acres.com/',
    #     'User-Agent'        : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36'
    # }

    def start_requests(self):
        self.log("Scrape initial", level=log.INFO)
        #get the links need to be scraped
        # links = self.get_lins()
        # for link in links:
            # print link['link']
            # print link['brand']
            # print link['door_type']

        url = "http://www.homedepot.com/s/masonite%2520doors"
        yield FormRequest(
            url,
            method="GET",
            meta={
                'site': "homedepot",
                'door_type': "door"
            },
            callback=self.data_choose_department
        )

    def data_choose_department(self, response):
        if response.status == 200:
            self.log("Access choose department page success.", level=log.INFO)
        else:
            self.log("choose department page can't access.", level=log.ERROR)
            raise CloseSpider(reason='Access fail and stop crawler (%s):%s' % (response.status, response.url))
        try:
            meta = response.request.meta
            site = meta['site']
            door_type = meta['door_type']
            url = response.xpath("//a[@data-refinementvalue='Doors & Windows']/@href").extract()[0]
            yield FormRequest(
                self.base_url + url,
                method="GET",
                meta={
                    'site': site,
                    'door_type': door_type
                },
                callback=self.data_start_parse
            )
        except Exception as exp:
            self.log("%s" % str(exp), level=log.ERROR)
            #save error page
            # file = open(rent_buy_new + '_' + city+'_'+str(current_page)+'.html', 'w')
            # file.write(response.body)
            # file.close()
            #finish saving  

    def data_start_parse(self, response):
        if response.status == 200:
            self.log("Access homedepot page success.", level=log.INFO)
        else:
            self.log("homedepot page can't access.", level=log.ERROR)
            raise CloseSpider(reason='Access fail and stop crawler (%s):%s' % (response.status, response.url))

        try:
            meta = response.request.meta
            site = meta['site']
            door_type = meta['door_type']
            next_page = response.xpath("//a[@class='icon-next']/@href").extract()
            next_page = " ".join(next_page).strip()
            print next_page

            total = response.xpath("//a[@id='all_products']/label[1]/text()").extract()[0]
            doors = response.xpath("//form[@name='comparisonform']")
            for door in doors:
                price = door.xpath(".//div[@class='item_pricing_wrapper']/span[1]/text()").extract()
                price = "".join(price) if price else '(None)'
                if price != '(None)':
                    price = price.strip()[1:]
    
                name = door.xpath(".//a[@class='item_description']/text()").extract()[0].strip()
                link = door.xpath(".//a[@class='item_description']/@href").extract()[0].strip()
                model = door.xpath(".//p[@class='model_container']//span/text()").extract()
                model = " ".join(model)

                item = DoorsHomedepotItem()

                item['site']        = site
                item['door_type']   = door_type
                item['brand']       = ""
                item['name']        = name
                item['price']       = price
                item['link']        = link
                item['total']       = total
                item['model']       = model
                
                yield item
            if next_page != '':
                yield FormRequest(
                    self.base_url + next_page,
                    method="GET",
                    meta={
                        'site': site,
                        'door_type': door_type
                    },
                    callback=self.data_start_parse
                )

        except Exception as exp:
            self.log("%s" % str(exp), level=log.ERROR)
            #save error page
            # file = open(rent_buy_new + '_' + city+'_'+str(current_page)+'.html', 'w')
            # file.write(response.body)
            # file.close()
            #finish saving  