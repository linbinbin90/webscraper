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

from doors_lowes.items import DoorsLowesItem

import random
import struct
import socket
import MySQLdb


class webscraping_lowesSpider(BaseSpider):
    # -- scrapy build in attributes
    name = "doors_lowes"
    allowed_domains = ["lowes.com"]
    base_url = "http://www.lowes.com"


    headers = {
        'Accept'            : 'text/javascript, text/html, application/xml, text/xml, */*',
        'Accept-Encoding'   : 'gzip,deflate',
        'Accept-Language'   : 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2,zh-CN;q=0.2',
        'Content-type'      : 'application/x-www-form-urlencoded; charset=UTF-8',
        'Connection'        : 'keep-alive',
        'Host'              : 'http://www.99acres.com/',
        'User-Agent'        : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36'
    }

    def start_requests(self):
        self.log("Scrape initial", level=log.INFO)
        #get the links need to be scraped
        # links = self.get_lins()
        # for link in links:
            # print link['link']
            # print link['brand']
            # print link['door_type']

        
        yield Request(
            self.base_url,
            method="GET",
            meta={
                'site': "lowes",
                'door_type': "door"
            },
            callback=self.data_choose_department
        )

    def data_choose_department(self, response):
        if response.status == 200:
            self.log("Access home page success.", level=log.INFO)
        else:
            self.log("Home page can't access.", level=log.ERROR)
            raise CloseSpider(reason='Access fail and stop crawler (%s):%s' % (response.status, response.url))
        try:
            meta = response.request.meta
            site = meta['site']
            door_type = meta['door_type']
            storeId = response.xpath("//input[@name='storeId']/@value").extract()[0]
            langId = response.xpath("//input[@name='langId']/@value").extract()[0]
            catalogId = response.xpath("//input[@name='catalogId']/@value").extract()[0]
            N = response.xpath("//input[@name='N']/@value").extract()[0]
            newSearch = response.xpath("//input[@name='newSearch']/@value").extract()[0]

            # url = self.base_url+'/Search=interior+doors?storeId='+storeId+'&langId='+langId+'&catalogId='+catalogId+'&N='+N+'&newSearch='+newSearch+'&Ntt=interior+doors'
            url = 'http://www.lowes.com:80/Windows-Doors/Doors/Interior-Doors/_/N-1z11qz6/pl?rpp=32&UserSearch=interior+doors'
            yield Request(
                url,
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
        print response.status
        print response.url

        file = open('test.html', 'w')
        file.write(response.body)
        file.close()
        return

        try:
            
            meta = response.request.meta
            site = meta['site']
            door_type = meta['door_type']
            next_page = response.xpath("//a[@title='Next Page']/@href").extract()[0]
            # next_page = " ".join(next_page).strip()
            print next_page

            total = response.xpath("//span[@class='numResults']/text()").extract()[0]
            print total
            return
            
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