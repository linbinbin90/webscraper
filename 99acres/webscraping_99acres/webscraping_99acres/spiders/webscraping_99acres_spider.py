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

from webscraping_99acres.items import Webscraping99AcresItem

import random
import struct
import socket


class webscraping_99acresSpider(BaseSpider):
    # -- scrapy build in attributes
    name = "webscraping_99acres"
    allowed_domains = ["99acres.com"]

    # city_list = {
    #     'delhi'     : {'name': 'Delhi'              }, 
    #     'mumbai'    : {'name': 'Mumbai (Bombay)'    },
    #     'kolkata'   : {'name': 'Kolkata (Calcutta)' },
    #     'chennai'   : {'name': 'Chennai (Madras)'   },
    #     'bangalore' : {'name': 'Bangalore'          },
    #     'hyderabad' : {'name': 'Hyderabad'          },
    #     'ahmedabad' : {'name': 'Ahmedabad'          },
    #     'pune'      : {'name': 'Pune'               },
    #     'surat'     : {'name': 'Surat'              },
    #     'jaipur'    : {'name': 'Jaipur'             }
    # }

    # url4sale = ['/property-in-kolkata-ffid',
    #           '/property-in-chennai-ffid',
    #           '/property-in-bangalore-ffid',
    #           '/property-in-hyderabad-ffid',
    #           '/property-in-ahmedabad-ffid',
    #           '/property-in-pune-ffid',
    #           '/property-in-surat-ffid',
    #           '/property-in-jaipur-ffid', 
    #           '/property-in-mumbai-ffid',
    #           '/property-in-delhi-ncr-ffid']
    #-- list for buy
    cities = {
        'Kolkata': '/property-in-kolkata-ffid',
        'Chennai': '/property-in-chennai-ffid',
        'Bangalore': '/property-in-bangalore-ffid',
        'Hyderabad': '/property-in-hyderabad-ffid',
        'Ahmedabad': '/property-in-ahmedabad-ffid',
        'Pune': '/property-in-pune-ffid',
        'Surat': '/property-in-surat-ffid',
        'Jaipur': '/property-in-jaipur-ffid',
        'Mumbai': '/property-in-mumbai-ffid',
        'Delhi': '/property-in-delhi-ncr-ffid'
    }
    #-- list for rent
    rent = {
        'Kolkata': '/rent-property-in-kolkata-ffid',
        'Chennai': '/rent-property-in-chennai-ffid',
        'Bangalore': '/rent-property-in-bangalore-ffid',
        'Hyderabad': '/rent-property-in-hyderabad-ffid',
        'Ahmedabad': '/rent-property-in-ahmedabad-ffid',
        'Pune': '/rent-property-in-pune-ffid',
        'Surat': '/rent-property-in-surat-ffid',
        'Jaipur': '/rent-property-in-jaipur-ffid',
        'Mumbai': '/rent-property-in-mumbai-ffid',
        'Delhi': '/rent-property-in-delhi-ncr-ffid'
    }
    #-- list for new project
    project = {
        'Kolkata': '/new-projects-in-kolkata-ffid',
        'Chennai': '/new-projects-in-chennai-ffid',
        'Bangalore': '/new-projects-in-bangalore-ffid',
        'Hyderabad': '/new-projects-in-hyderabad-ffid',
        'Ahmedabad': '/new-projects-in-ahmedabad-ffid',
        'Pune': '/new-projects-in-pune-ffid',
        'Surat': '/new-projects-in-surat-ffid',
        'Jaipur': '/new-projects-in-jaipur-ffid',
        'Mumbai': '/new-projects-in-mumbai-ffid',
        'Delhi': '/new-projects-in-delhi-ncr-ffid'
    }

    # headers = {
    #     'Accept'            : 'text/javascript, text/html, application/xml, text/xml, */*',
    #     'Accept-Encoding'   : 'gzip,deflate',
    #     'Accept-Language'   : 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2,zh-CN;q=0.2',
    #     'Content-type'      : 'application/x-www-form-urlencoded; charset=UTF-8',
    #     'Connection'        : 'keep-alive',
    #     'Host'              : 'http://www.99acres.com/',
    #     'User-Agent'        : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36'
    # }

    def start_requests(self):
        self.log("Scrape initial", level=log.INFO)

        for city, link in self.cities.iteritems():
            self.log("---Start requests for city: %s  for BUY ---" % city, level=log.INFO)

            #--Buy request
            url = 'http://www.99acres.com' + str(link)
            rand_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
            yield FormRequest(
                url,
                method="GET",
                headers ={
                    "X-Forwarded-For": rand_ip
                },
                # formdata = {
                #     "search_type"           : 'QS',
                #     # "search_location"       : 'NRI',
                #     # "lstAcn"                : 'NR_R',
                #     # "lstActId"              : '-1',
                #     "src"                   : 'CLUSTER',
                #     "isvoicesearch"         : 'N',
                #     "keyword"               : city,
                #     "class"                 : 'O,B,A',
                #     "strEntityMap"          : 'IiI=',
                #     "refine_results"        : 'Y',
                #     "Refine_Localities"     : 'Refine Localities',
                #     "action"                : '/do/quicksearch/search',
                #     "property_type"         : '1,4,2,3,90,5,22,80'
                # },
                meta={
                    'city': city,
                    'rent_buy_new': 'buy',
                    'current_page': 1
                },
                callback=self.data_start_parse
            )

        for city, link in self.rent.iteritems():
            self.log("---Start requests for city: %s for RENT ---" % city, level=log.INFO)

            #--Rent request

            url = 'http://www.99acres.com' + str(link)
            rand_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
            yield FormRequest(
                url,
                headers ={
                    "X-Forwarded-For": rand_ip
                },
                method="GET",
                # formdata = {
                #     "is_sem_page_request"     : 'false',
                #     "search_intent"           : 'sale',
                #     "city"                    : city,
                #     "builtup_area_min"        : 'Min',
                #     "builtup_area_max"        : 'Max',
                #     "property_location_input" : 'Select Location',
                #     "page"                    : '1',
                #     "srtby"                   : 'bestquality'
                # },
                meta={
                    'city': city,
                    'rent_buy_new': 'rent',
                    'current_page': 1
                },
                callback=self.data_start_parse
            )

        for city, link in self.project.iteritems():
            self.log("---Start requests for city: %s for NEW PROJECT ---" % city, level=log.INFO)

            #--new project request

            url = 'http://www.99acres.com' + str(link)
            rand_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
            yield FormRequest(
                url,
                headers ={
                    "X-Forwarded-For": rand_ip
                },
                method="GET",
                # formdata = {
                #     "is_sem_page_request"     : 'false',
                #     "search_intent"           : 'sale',
                #     "city"                    : city,
                #     "builtup_area_min"        : 'Min',
                #     "builtup_area_max"        : 'Max',
                #     "property_location_input" : 'Select Location',
                #     "page"                    : '1',
                #     "srtby"                   : 'bestquality'
                # },
                meta={
                    'city': city,
                    'rent_buy_new': 'new',
                    'current_page': 1
                },
                callback=self.newProject_data_start_parse
            )



    def newProject_data_start_parse(self, response):
        # if response.status == 200:
        #     self.log("Access buy_data_start_parse success.", level = log.INFO)
        # else:
        #     self.log("Access buy_data_start_parse.", level = log.ERROR)
        #     raise CloseSpider( reason = 'Access fail and stop crawler (%s):%s' % (response.status, response.url) )
        try:
            meta = response.request.meta
            current_page = meta['current_page']
            city = meta['city']
            rent_buy_new = meta['rent_buy_new']

            self.log("%s - City: %s ,Page: %s ,Status Code: %s" % (rent_buy_new, city, current_page, response.status), level=log.INFO)
            # -- save total number
            buy_total_num_response = response.xpath("//div[@class='sRTabs']//a[@id='np_link']/text()").extract()[0].split()[0]
            self.log("%s - City: %s, have %s totally records" % (rent_buy_new, city, buy_total_num_response), level=log.INFO) 
            #-- parse the next link, if the last page, it will be 0
            next = response.xpath("//a[@class='pgsel'][contains(concat(' ', @value, ' '), 'Next ')]/@href").extract()
            # next = response.xpath("//a[@class='pgsel'][contains(concat(' ', @value, ' '), 'Next ')]").extract()
            if(len(next) == 0):
                next = '0'
            else:
                next = next[0]
            print next
            self.log("%s - City: %s, next page link: %s" % (rent_buy_new, city, next), level=log.INFO)

            # #-- parse  every property directly


            results_response = response.xpath("//div[contains(concat(' ', @class, ' '), 'npsrp ')]")
            #-- if response is not NULL, continue to Scrapy until the response is NULL 
            # print len(results_response)
            if len(results_response) != 0:
                for result in results_response:

                    # -- parse the area size
                    # print "start parse "
                    try:
                        result_price = result.xpath(".//div[@class='f18 lf']//div[1]/text()").extract()
                        result_price = "".join(result_price).strip().replace(',', '') if result_price else '(None)'

                        if('upto' in result_price):
                            result_price = result_price[5:]
                        if('to' in result_price):
                            price_start, price_end = result_price.split('to')
                            price_start =  price_start.strip().split()
                            price_end = price_end.strip().split()
                            if(len(price_start) == 1):
                                result_price = str((float(price_start[0]) + float(price_end[0]))/2)
                            else:
                                result_price = str((float(price_start[0]) + float(price_end[0]))/2) + " " + price_start[1]
                        # print result_price
                    except Exception as exp:
                        self.log("Weried result price: %s" % result_price, level=log.ERROR)
                        result_price = '(None)'

                    #-- parse the price
                    try:
                        result_size = result.xpath(".//div[@class='lf mt30']//table[1]//tr//td[3]/text()").extract()
                        result_size = " ".join(result_size).replace('-', '').split() if result_size else '(None)'
                        num = []
                        for size in result_size:
                            try:
                                num.append(size)
                            except Exception as exp:
                                None
                        # print num
                        if(len(num) < 1):
                            result_size = '(None)'
                        elif(len(num) == 1):
                            result_size = str(num[0]).replace(',', '') + ' ' + result_size[len(result_size)-1]
                        else:
                            # result_size = str(num[0]) + '-' + str(num[len(num)-1]) + ' ' + result_size[len(result_size)-1]
                            result_size = str((float(num[0].replace(',', '')) + float(num[len(num)-2].replace(',', '')))/2) + ' ' + result_size[len(result_size)-1]
                        # print result_size
                    except Exception as exp:
                        self.log("Weried result size: %s" % result_size, level=log.ERROR)
                        result_size = '(None)'


            #         #-- parse the post date
            #         result_date = result.xpath(".//div[@class='rf f11 mt25']/text()").extract()
            #         result_date = result_date[0].strip() if result_date else '(None)'
            #         if result_date != '(None)':
            #             result_date = result_date[result_date.find('Posted') + 9 : ]
            #         # print result_date

                    #-- parse the locality
                    result_locality = result.xpath(".//a[@class='npt_titl_desc']/text()").extract()
                    result_locality = result_locality[0].strip() if result_locality else None
                    # print result_locality

                    #-- parse the item_link
                    result_item_link = result.xpath(".//a[1]/@href").extract()
                    result_item_link = result_item_link[0].strip() if result_item_link else None
                    # print result_item_link

                    item = Webscraping99AcresItem()

                    item['price']        = self.price_converter(result_price)
                    item['size']         = self.size_converter(result_size)
                    item['date']         = None
                    item['locality']     = result_locality
                    item['item_link']    = result_item_link
                    item['city']         = city
                    item['total']        = buy_total_num_response
                    item['rent_buy_new'] = rent_buy_new
                    item['city_link']    = response.url

                    yield item
        except Exception as exp:
            self.log("%s"  % str(exp),  level = log.ERROR)
            #save error page
            file = open(rent_buy_new + '_' + city+'_'+str(current_page)+'.html', 'w')
            file.write(response.body)
            file.close()
            #finish saving

        if(next == '0'):
            return
        else:
            url = 'http://www.99acres.com' + str(next)
            rand_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
            yield FormRequest(
                url,
                headers = {
                    "X-Forwarded-For": rand_ip
                },
                method="GET",
                # formdata = {
                #     "is_sem_page_request"     : 'false',
                #     "search_intent"           : 'sale',
                #     "city"                    : city,
                #     "builtup_area_min"        : 'Min',
                #     "builtup_area_max"        : 'Max',
                #     "property_location_input" : 'Select Location',
                #     "page"                    : '1',
                #     "srtby"                   : 'bestquality'
                # },
                meta={
                    'city': city,
                    'rent_buy_new': rent_buy_new,
                    'current_page': int(current_page) + 1
                },
                callback=self.newProject_data_start_parse
            )




    def data_start_parse(self, response):
        # if response.status == 200:
        #     self.log("Access data_start_parse success.", level = log.INFO)
        # else:
        #     self.log("Access data_start_parse.", level = log.ERROR)
        #     raise CloseSpider( reason = 'Access fail and stop crawler (%s):%s' % (response.status, response.url) )
        try:

            meta = response.request.meta
            current_page = meta['current_page']
            city = meta['city']
            rent_buy_new = meta['rent_buy_new']

            #save error page
            # file = open(rent_buy_new + '_' + city+'_'+str(current_page)+'.html', 'w')
            # file.write(response.body)
            # file.close()
            #finish saving

            self.log("%s - City: %s ,Page: %s ,Status Code: %s" % (rent_buy_new, city, current_page, response.status), level=log.INFO)
            # -- save total number
            buy_total_num_response = response.xpath("//div[@class='sRTabs']//a[@id='qs_link']/text()").extract()[0].split()[0]
            self.log("%s - City: %s, have %s totally records" % (rent_buy_new, city, buy_total_num_response), level=log.INFO) 
            #-- parse the next link, if the last page, it will be 0
            next = response.xpath("//a[@class='pgsel'][contains(concat(' ', @value, ' '), 'Next ')]/@href").extract()
            # next = response.xpath("//a[@class='pgsel'][contains(concat(' ', @value, ' '), 'Next ')]").extract()
            if(len(next) == 0):
                next = '0'
            else:
                next = next[0]
            print next
            self.log("%s - City: %s, next page link: %s" % (rent_buy_new, city, next), level=log.INFO)

            # #-- parse  every property directly

            # properties = response.xpath("//div[contains(concat(' ', @class, ' '), 'srpWrap ')]").extract
            # print properties

            results_response = response.xpath("//div[contains(concat(' ', @class, ' '), 'srpWrap ')]")
            #-- if response is not NULL, continue to Scrapy until the response is NULL 
            if len(results_response) != 0:
                for result in results_response:

                    #-- parse the area size
                    try:
                        result_size = result.xpath(".//div[@class='srpDataWrap']//span[1]//b[1]/text()").extract()
                        result_size = result_size[0].strip() if result_size else '(None)'
                        # print result_size
                    except Exception as exp:
                        self.log("Weried result size: %s" % result_size, level=log.ERROR)
                        result_size = '(None)'


                    #-- parse the price
                    try:
                        result_price = result.xpath(".//div[@class='wrapttl']//div[1]//b[2]/text()").extract()
                        result_price = result_price[0].strip().replace(',', '') if result_price else '(None)'
                        # print result_price
                        if('upto' in result_price):
                            result_price = result_price[5:]
                        if('to' in result_price):
                            price_start, price_end = result_price.split('to')
                            price_start =  price_start.strip().split()
                            price_end = price_end.strip().split()
                            if(len(price_start) == 1):
                                result_price = str((float(price_start[0]) + float(price_end[0]))/2)
                            else:
                                result_price =  str((float(price_start[0]) + float(price_end[0]))/2) + " " + price_start[1]
                    except Exception as exp:
                        self.log("Weried result price: %s" % result_price, level=log.ERROR)
                        result_price = '(None)'


                    #-- parse the post date
                    try:
                        result_date = result.xpath(".//div[contains(concat(' ', @class, ' '), 'rf f11 mt25')]/text()").extract()
                        # result_date = result_date[0].strip()
                        if len(result_date) == 0:
                            result_date = result.xpath(".//div[@class = 'lf f13 hm10 mb5']/text()").extract()
                        result_date = result_date[0].strip() if result_date else '(None)'
                        if result_date != '(None)':
                            result_date = result_date[result_date.find('Posted') + 9 : ].strip()
                        # print result_date
                    except Exception as exp:
                        self.log("Don't have post date", level=log.ERROR)

                    #-- parse the locality
                    result_locality = result.xpath(".//div[@class='wrapttl']//div[1]//a[1]/@title").extract()
                    result_locality = result_locality[0].strip() if result_locality else '(None)'
                    # print result_locality

                    #-- parse the item_link
                    result_item_link = result.xpath(".//div[@class='wrapttl']//div[1]//a[1]/@href").extract()
                    result_item_link = result_item_link[0].strip() if result_item_link else '(None)'
                    # print result_item_link

                    item = Webscraping99AcresItem()

                    item['price']        = self.price_converter(result_price)
                    item['size']         = self.size_converter(result_size)
                    item['date']         = self.date_converter(result_date)
                    item['locality']     = result_locality
                    item['item_link']    = result_item_link
                    item['city']         = city
                    item['total']        = buy_total_num_response
                    item['rent_buy_new'] = rent_buy_new
                    item['city_link']    = response.url

                    yield item

        except Exception as exp:
            self.log("%s"  % str(exp),  level = log.ERROR)
            #save error page
            file = open(rent_buy_new + '_' + city+'_'+str(current_page)+'.html', 'w')
            file.write(response.body)
            file.close()
            #finish saving

        if(next == '0'):
            return
        else:
            url = 'http://www.99acres.com' + str(next)
            rand_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
            yield FormRequest(
                url,
                headers = {
                    "X-Forwarded-For": rand_ip
                },
                method="GET",
                # formdata = {
                #     "is_sem_page_request"     : 'false',
                #     "search_intent"           : 'sale',
                #     "city"                    : city,
                #     "builtup_area_min"        : 'Min',
                #     "builtup_area_max"        : 'Max',
                #     "property_location_input" : 'Select Location',
                #     "page"                    : '1',
                #     "srtby"                   : 'bestquality'
                # },
                meta={
                    'city': city,
                    'rent_buy_new': rent_buy_new,
                    'current_page': int(current_page) + 1
                },
                callback=self.data_start_parse
            )



    # def buy_data_detail_parse(self, response):
    def price_converter(self, price_string):
        if price_string:
            price_string = price_string.lower().replace(',', '')
            try:
                if price_string != '(none)':
                    if ('request' in price_string):
                        return None
                    elif 'cr' in price_string:
                        return float( re.search("[0-9.]+", price_string).group(0) ) * 10000000
                    elif 'lac' in price_string:
                        return float( re.search("[0-9.]+", price_string).group(0) ) * 1000000
                    elif 'lakh' in price_string:
                        return float( re.search("[0-9.]+", price_string).group(0) ) * 1000
                    else:
                        return float( re.search("[0-9.]+", price_string).group(0) )
                else:
                    return None
            except Exception as exp:
                self.log("Price convert error: %s" % price_string, level=log.ERROR)
                return None
        else:
            return None

    def size_converter(self, result_size):
        if result_size:
            result_size = result_size.lower().replace(',', '')
            if result_size != '(none)':     
                try:
                    if 'ft' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) )
                    elif 'yards' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 9
                    elif 'meter' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 10.764
                    elif 'kottah' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 720
                    elif 'ground' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 2400
                    elif 'acre' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 43560
                    elif 'bigha' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 27225
                    elif 'guntha' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 1088.98481685852
                    elif 'cent' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 435.6
                    elif 'chatak' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 450
                    elif 'marla' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 1785.96
                    elif 'are' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 1076.08
                    elif 'rood' in result_size:
                        return float( re.search("[0-9.]+", result_size).group(0) ) * 10890
                    else:
                        self.log("Size convert errror: %s" % result_size, level=log.ERROR)
                        return None
                except Exception as e:
                    self.log("Size convert error(%s):%s" % (result_size, e), level=log.ERROR)
                    return None
            else:
                return None
        else:
            return None

    def date_converter(self, result_date):
        if result_date:
            result_date = result_date.lower().replace(',', ' ')
            try:
                if result_date == '(none)':
                #-- Done: make the null return value correct
                    return None
                elif 'yesterday' in result_date:
                    date_obj = datetime.today() - timedelta(days=1)
                    return date_obj.strftime("%Y-%m-%d")
                elif 'today' in result_date:
                    date_obj = datetime.today()
                    return date_obj.strftime("%Y-%m-%d")
                else:
                    date_obj = datetime.strptime(result_date.rstrip() , "%b %d %Y")
                    return date_obj.strftime("%Y-%m-%d")
            except Exception as e:
                #-- trigger prase error
                #-- Done: make the null return value correct
                self.log("Parse date error at (%s): %s" % (result_date, e), level=log.ERROR )
                return None
        else:
            return None





