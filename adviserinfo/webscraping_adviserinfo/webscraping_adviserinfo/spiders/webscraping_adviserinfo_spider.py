# -*- coding: utf-8 -*-

from scrapy import log
from scrapy.spider import Spider
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.exceptions import CloseSpider
import re
from webscraping_adviserinfo.items import WebscrapingAdviserinfoItem

import random
import struct
import socket
import csv
from const import *
import time
import util
import datetime
import MySQLdb

class webscraping_AdviserinfoSpider(Spider):
    name = "webscraping_adviserinfo"
    allowed_domains = ["www.adviserinfo.sec.gov"]
    start_url = "http://www.adviserinfo.sec.gov/IAPD/Content/Search/iapd_Search.aspx"
    base_url = "http://www.adviserinfo.sec.gov"
    # headers = {
    #             'Accept'            : 'text/javascript, text/html, application/xml, text/xml, */*',
    #             'Accept-Encoding'   : 'gzip,deflate',
    #             'Accept-Language'   : 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2,zh-CN;q=0.2',
    #             'Content-type'      : 'application/x-www-form-urlencoded; charset=UTF-8',
    #             'Connection'        : 'keep-alive',
    #             'Host'              : 'www.adviserinfo.sec.gov',
    #             'User-Agent'        : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36'
    #         }

    # zipcodeList = ['00704', '00705', '00707', '00714', '00715', '00716', '00717', '00718', '00719', '00720', '00721', '00723', '00725', '00726', '00728', '00729', '00730', '00731', '00732', '00733', '00734', '00735', '00736', '00737', '00738', '00739', '00740', '00741', '00742', '00744', '00745', '00747', '00748', '00751', '00752', '00754', '00757', '00761', '00762', '00763', '00764', '00765', '00766', '00767', '00768', '00769', '00771', '00772', '00773', '00775', '00777', '00778', '00780', '00782', '00783', '00784', '00785', '00786', '00791', '00792', '00794', '00795', '00901', '00902', '00906', '00907', '00908', '00909', '00910', '00911', '00912', '00913', '00914', '00915', '00916', '00917', '00918', '00919', '00920', '00921', '00922', '00923', '00924', '00925', '00926', '00927', '00928', '00929', '00930', '00931', '00933', '00934', '00935', '00936', '00937', '00938', '00939', '00940', '00949', '00950', '00951', '00952', '00953', '00954', '00955', '00956', '00957', '00958', '00959', '00960', '00961', '00962', '00963', '00965', '00966', '00968', '00969', '00970', '00971', '00975', '00976', '00977', '00978', '00979', '00981', '00982', '00983', '00984', '00985', '00986', '00987', '00988']
    curr_id = -1
    zipcodeList = []
    companyList = []
    headers = {}

    db_conn = MySQLdb.connect(
        host='127.0.0.1',
        user='root',
        passwd='900129lbb',
        db='project_adviserinfo_crawling',
        charset='utf8'
    )

    def __init__(self, zipcode, DB_table, *args, **kwargs):
         super(webscraping_AdviserinfoSpider, self).__init__(*args, **kwargs)
         self.zipcodeList.append(zipcode)
         self.headers = util.get_headers()
         self.result_table = DB_table
         # print zipcode

    def start_requests(self):
        # zipcodeList = ['00669', '00670', '00671', '00674', '00676', '00677', '00678', '00680', '00681', '00682', '00683', '00685', '00687', '00688', '00690', '00692', '00693', '00694', '00698', '00703', '00704', '00705', '00707', '00714', '00715', '00716', '00717', '00718', '00719', '00720', '00721', '00723', '00725', '00726', '00728', '00729', '00730', '00731', '00732', '00733', '00734', '00735', '00736', '00737', '00738', '00739', '00740', '00741', '00742', '00744', '00745', '00747', '00748', '00751', '00752', '00754', '00757', '00761', '00762', '00763', '00764', '00765', '00766', '00767', '00768', '00769', '00771', '00772', '00773', '00775', '00777', '00778', '00780', '00782', '00783', '00784', '00785', '00786', '00791', '00792', '00794', '00795', '00901', '00902', '00906', '00907', '00908', '00909', '00910', '00911', '00912', '00913', '00914', '00915', '00916', '00917', '00918', '00919', '00920', '00921', '00922', '00923', '00924', '00925', '00926', '00927', '00928', '00929', '00930', '00931', '00933', '00934', '00935', '00936', '00937', '00938', '00939', '00940', '00949', '00950', '00951', '00952', '00953', '00954', '00955', '00956', '00957', '00958', '00959', '00960', '00961', '00962', '00963', '00965', '00966', '00968', '00969', '00970', '00971', '00975', '00976', '00977', '00978', '00979', '00981', '00982', '00983', '00984', '00985', '00986', '00987', '00988']
        
        # with open('./zipcode.csv', 'rb') as csvfile:
        #     spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        #     for row in spamreader:
        #         if row[2] == '"PR"':
        #             if int(row[0][1:len(row[0])-1]) == 601:
        #                 self.zipcodeList.append(row[0][1:len(row[0])-1])

        # self.zipcodeList.append(zipcode)

        # print self.zipcodeList

        # print self.zipcodeList
        # for i in range(0, len(self.zipcodeList)):
        #     print self.zipcodeList[i]
        # yield FormRequest(
        #     self.start_url,
        #     method="GET",
        #     # headers = self.headers,
        #     meta={
        #         'id': 0
        #     },
        #     callback=self.data_parse_l1
        # )
        yield self.trigger_next_zipcode()
        

    def err_callback(self, response):
        self.conn = self.db_conn
        self.cursor = self.conn.cursor()
        self.cursor.execute("""SELECT * FROM err_records WHERE zipcode = (%s)""",
                                (
                                    self.zipcodeList[0], 
                                )
            )
        rows = self.cursor.fetchall()
        if len(rows) == 0:
            self.cursor.execute("""INSERT INTO err_records (datetime, zipcode)
                        VALUES ( %s, %s )""",
                                    (
                                        str(time.strftime("%Y-%m-%d %H:%M:%S")),
                                        self.zipcodeList[0],
                                    )
                )
            self.conn.commit()
        self.cursor.close()
        raise CloseSpider( reason = 'request error' )

    def err_handle(self):
        self.conn = self.db_conn
        self.cursor = self.conn.cursor()
        self.cursor.execute("""SELECT * FROM err_records WHERE zipcode = (%s)""",
                                (
                                    self.zipcodeList[0], 
                                )
            )
        rows = self.cursor.fetchall()
        if len(rows) == 0:
            self.cursor.execute("""INSERT INTO err_records (datetime, zipcode)
                        VALUES ( %s, %s )""",
                                    (
                                        str(time.strftime("%Y-%m-%d %H:%M:%S")),
                                        self.zipcodeList[0],
                                    )
                )
            self.conn.commit()
        self.cursor.close()
        raise CloseSpider( reason = 'request error' )
        
    def trigger_next_zipcode(self):
        # print 'trigger next zipcode'

        #-- because the website only running from 11PM to 7AM
        # now = datetime.datetime.now()
        # startTime = now.replace(hour = 3, minute = 30, second = 0, microsecond = 0)
        # endTime = now.replace(hour = 5, minute = 0, second = 0, microsecond = 0)
        # if now <= endTime and now >= startTime:
        #     self.log("sleep for 2 hours", level=log.INFO) 
        #     time.sleep(7200)
        #-- end of sleep

        if self.curr_id+1 >= len(self.zipcodeList):
            self.log('All zipcode finish!', level=log.INFO)
        else:
            self.curr_id += 1
            # print self.zipcodeList[self.curr_id]

            return FormRequest(
                self.start_url,
                method="GET",
                headers = self.headers,
                # headers = util.get_headers(),
                meta={
                    'id': self.curr_id,
                    'zipcode' : self.zipcodeList[self.curr_id]
                },
                callback=self.data_parse_l1,
                errback = self.err_callback,
                dont_filter=True
            )
            
    def data_parse_l1(self, response):
        # print 'enter the level 1'
        # if response.url == webblock: closeSpider
        if response.url == 'http://www.adviserinfo.sec.gov/IAPD/WebLock/WebLock.aspx':
            self.err_handle()

        #-- because the website only running from 11PM to 7AM
        # now = datetime.datetime.now()
        # startTime = now.replace(hour = 3, minute = 30, second = 0, microsecond = 0)
        # endTime = now.replace(hour = 5, minute = 0, second = 0, microsecond = 0)
        # if now <= endTime and now >= startTime:
        #     self.log("sleep for 2 hours", level=log.INFO) 
        #     time.sleep(7200)
        #-- end of sleep
        # print response.url
        meta = response.request.meta
        # curr_id = meta['id']
        zipcode = meta['zipcode']
        # print zipcode
        try:
            viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").extract()[0]
            eventvalidation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract()[0]
            viewstategenerator = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").extract()[0]
        
        
        
            # print eventvalidation
            # print viewstategenerator
            # print viewstate
            yield FormRequest(
                self.start_url,
                # headers = {
                #     "X-Forwarded-For": rand_ip
                # },
                headers = self.headers,
                # headers = util.get_headers(),
                method="POST",
                formdata = {
                    "__EVENTTARGET"         : 'ctl00$cphMainContent$ucUnifiedSearch$rdoOrg',
                    "__VIEWSTATE"           : viewstate,
                    "__VIEWSTATEGENERATOR"  : viewstategenerator,
                    "__EVENTVALIDATION"     : eventvalidation,
                    "ctl00$cphMainContent$ucUnifiedSearch$rdoSearchBy"    : 'rdoOrg'
                },
                meta={
                    # 'current_page': 1
                    # 'id' : curr_id,
                    'zipcode' : zipcode
                },
                callback=self.data_parse_l2,
                errback = self.err_callback,
                dont_filter=True
            )
        except Exception, e:
            #save error page
            file = open(str(zipcode) + '_l1.html', 'w')
            file.write(response.body)
            file.close()
            #finish saving
            self.err_handle()

    def data_parse_l2(self, response):
        # print 'enter the level 2'
        if response.url == 'http://www.adviserinfo.sec.gov/IAPD/WebLock/WebLock.aspx':
            self.err_handle()
        #-- because the website only running from 11PM to 7AM
        # now = datetime.datetime.now()
        # startTime = now.replace(hour = 3, minute = 30, second = 0, microsecond = 0)
        # endTime = now.replace(hour = 5, minute = 0, second = 0, microsecond = 0)
        # if now <= endTime and now >= startTime:
        #     self.log("sleep for 2 hours", level=log.INFO)
        #     time.sleep(7200)
        #-- end of sleep

        # print response.status
        # print response.url
        meta = response.request.meta
        # curr_id = meta['id']
        zipcode = meta['zipcode']
        # print zipcode
        #save error page
        # file = open('l2.html', 'w')
        # file.write(response.body)
        # file.close()
        #finish saving
        try:
            viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").extract()[0]
            eventvalidation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract()[0]
            viewstategenerator = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").extract()[0]
        
            # viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").extract()[0]
            # eventvalidation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract()[0]
            # viewstategenerator = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").extract()[0]
            yield FormRequest(
                self.start_url,
                # headers = {
                #     "X-Forwarded-For": rand_ip
                # },
                headers = self.headers,
                # headers = util.get_headers(),
                method="POST",
                formdata = {
                    "__EVENTTARGET"         : 'ctl00$cphMainContent$ucUnifiedSearch$rdoOrg',
                    "__VIEWSTATE"           : viewstate,
                    "__VIEWSTATEGENERATOR"  : viewstategenerator,
                    "__EVENTVALIDATION"     : eventvalidation,
                    "ctl00$cphMainContent$ucUnifiedSearch$txtZip" : zipcode,
                    "ctl00$cphMainContent$ucUnifiedSearch$ddlZipRange" : '15',
                    "ctl00$cphMainContent$ucUnifiedSearch$btnFreeFormSearch" : 'Start Search',
                    "ctl00$cphMainContent$ucUnifiedSearch$rdoSearchBy"    : 'rdoOrg'
                },
                meta={
                    # 'current_page': 1
                    # 'id' : curr_id,
                    'zipcode' : zipcode
                },
                callback=self.data_parse_list,
                errback = self.err_callback,
                dont_filter=True
            )
        except Exception, e:
            #save error page
            file = open(str(zipcode) + '_l2.html', 'w')
            file.write(response.body)
            file.close()
            #finish saving

    def data_parse_list(self, response):
        # print 'enter the level 3, parse the company list'
        if response.url == 'http://www.adviserinfo.sec.gov/IAPD/WebLock/WebLock.aspx':
            self.err_handle()
        #-- because the website only running from 11PM to 7AM
        # now = datetime.datetime.now()
        # startTime = now.replace(hour = 3, minute = 30, second = 0, microsecond = 0)
        # endTime = now.replace(hour = 5, minute = 0, second = 0, microsecond = 0)
        # if now <= endTime and now >= startTime:
        #     self.log("sleep for 2 hours", level=log.INFO)
        #     time.sleep(7200)
        #-- end of sleep

        # print response.status
        # print response.url
        try:
            meta = response.request.meta
            # curr_id = meta['id']
            zipcode = meta['zipcode']
            # print zipcode

            viewstate = response.xpath("//input[@id='__VIEWSTATE']/@value").extract()[0]
            eventvalidation = response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract()[0]
            viewstategenerator = response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").extract()[0]
            nextPage = response.xpath("//input[@id='ctl00_cphMainContent_grOrgResults_ctl01_btnNextPage']/@value").extract()
            nextPage = nextPage[0].strip() if nextPage else '(None)'
            # print nextPage
            #save error page
            # file = open(zipcode+'_l3.html', 'w')
            # file.write(response.body)
            # file.close()
            #finish saving
            links = []
            
            ADVLinks = response.xpath("//a[@title='Link to Form ADV']/@href").extract()
            if len(ADVLinks) != 0:
                for link in ADVLinks:
                    links.append(link[ link.find("javascript:JSetAndSub('")+23 : link.find("','")])
                # print links
                if len(links) != 0:
                    i = 0
                    for link in links:
                        i += 1
                    # for i in range(0, len(links)):
                        url = self.base_url + link
                        if nextPage == '(None)' and i == len(links) :
                            isEnd = '1'
                        else:
                            isEnd = '0' #-- 0 is false, 1 is true
                        #-- get the ORG_PK value which is equal to FirmKey value in link
                        ORG_PK = ''
                        words = link.split('&')
                        for word in words:
                            if 'FirmKey' in word:
                                ORG_PK = word[8:]
                                
                                break
                        # print ORG_PK
                        # url = 'http://www.adviserinfo.sec.gov/IAPD/Content/Search/iapd_landing.aspx?SearchGroup=Firm&FirmKey=25454&BrokerKey=-1'
                        # print 'level 3 request url: ' + url
                        yield FormRequest(
                            url,
                            # headers = {
                            #     "X-Forwarded-For": rand_ip
                            # },
                            headers = self.headers,
                            # headers = util.get_headers(),
                            method="POST",
                            formdata = {
                                "PageType"         : 'Search',
                                "ORG_PK"           : ORG_PK,
                                "STATE_CD"  : 'undefined'
                            },
                            meta={
                                # 'current_page': 1
                                # 'id' : curr_id,
                                'zipcode' : zipcode,
                                'isEnd' : isEnd
                            },
                            callback=self.data_parse_company,
                            dont_filter=True
                        )

            #-- go to next page
            if nextPage != '(None)':
                url = response.url
                yield FormRequest(
                    url,
                    # headers = {
                    #     "X-Forwarded-For": rand_ip
                    # },
                    headers = self.headers,
                    # headers = util.get_headers(),
                    method="POST",
                    formdata = {
                        "__VIEWSTATE"           : viewstate,
                        "__VIEWSTATEGENERATOR"  : viewstategenerator,
                        "__EVENTVALIDATION"     : eventvalidation,
                        "ctl00$cphMainContent$grOrgResults$ctl01$btnNextPage" : '>>',
                        "ctl00$cphMainContent$grOrgResults$ctl01$ddlPageSize" : '25',
                        "ctl00$cphMainContent$grOrgResults$ctl29$ddlPageSize" : '25'
                    },
                    meta={
                        # 'current_page': 1
                        # 'id' : curr_id,
                        'zipcode' : zipcode
                    },
                    callback=self.data_parse_list,
                    errback = self.err_callback,
                    dont_filter=True
                )
            else:
                self.log("Finish the zipcode: %s" % zipcode, level=log.INFO)
                yield self.trigger_next_zipcode()
        except Exception as exp:
            # print "no result triger the next zipcode"
            self.log("Finish the zipcode: %s" % zipcode, level=log.INFO)
            # print exp
            yield self.trigger_next_zipcode()

    def data_parse_company(self, response):
        # print 'choose a state or sec and continue'
        if response.url == 'http://www.adviserinfo.sec.gov/IAPD/WebLock/WebLock.aspx':
            self.err_handle()
        #-- because the website only running from 11PM to 7AM
        # now = datetime.datetime.now()
        # startTime = now.replace(hour = 3, minute = 30, second = 0, microsecond = 0)
        # endTime = now.replace(hour = 5, minute = 0, second = 0, microsecond = 0)
        # if now <= endTime and now >= startTime:
        #     self.log("sleep for 2 hours", level=log.INFO)
        #     time.sleep(7200)
        #-- end of sleep


        # print response.url
        meta = response.request.meta
        # curr_id = meta['id']
        zipcode = meta['zipcode']
        isEnd = meta['isEnd']
        #save error page
        # file = open(zipcode+'_l4.html', 'w')
        # file.write(response.body)
        # file.close()
        #finish saving
        
        companyName = response.xpath("//span[@id='ctl00_cphMainContent_lblActiveOrgName']/text()").extract()
        if companyName:
            companyName = companyName[0]
        else:
            return
        if companyName in self.companyList:
            return
        self.companyList.append(companyName)

        self.conn = self.db_conn
        self.cursor = self.conn.cursor()
        self.cursor.execute("""SELECT * FROM """ + self.result_table + """ WHERE name = (%s)""",
                                (
                                    companyName, 
                                )
            )
        rows = self.cursor.fetchall()
        self.cursor.close()
        if len(rows) != 0:
            return

        links = response.xpath("//table[@id='tblActiveOrg']//a/@href").extract()
        # print links
        # print links
        i = 0
        # for link in links:
            # i += 1
        if len(links) != 0:
        # for i in range(0, len(links)):
            url = self.base_url + '/IAPD/' + links[i][6:]
            # print url
            # print "before isEnd: " + isEnd
            if isEnd == '1' and i == len(links):
                check = '1'
            else:
                check = '0'
            # print "after check: " + str(check) + "  i: " + str(i) + "  links lenght: "+ str(len(links))
            yield FormRequest(
                url,
                # headers = {
                #     "X-Forwarded-For": rand_ip
                # },
                headers = self.headers,
                method="GET",
                meta={
                    # 'current_page': 1
                    # 'id' : curr_id,
                    'zipcode' : zipcode,
                    'companyName' : companyName,
                    'isEnd' : check
                },
                callback=self.data_parse_detail,
                errback = self.err_callback
            )


    def data_parse_detail(self, response):
        # print 'enter the detail of company'
        if response.url == 'http://www.adviserinfo.sec.gov/IAPD/WebLock/WebLock.aspx':
            self.err_handle()

        #-- because the website only running from 11PM to 7AM
        # now = datetime.datetime.now()
        # startTime = now.replace(hour = 3, minute = 30, second = 0, microsecond = 0)
        # endTime = now.replace(hour = 5, minute = 0, second = 0, microsecond = 0)
        # if now <= endTime and now >= startTime:
        #     self.log("sleep for 2 hours", level=log.INFO)
        #     time.sleep(7200)
        #-- end of sleep

        prevUrl = response.url
        meta = response.request.meta
        # curr_id = meta['id']
        companyName = meta['companyName']
        isEnd = meta['isEnd']
        zipcode = meta['zipcode']
        # print zipcode
        #save error page
        # file = open('detail.html', 'w')
        # file.write(response.body)
        # file.close()
        #finish saving
        url = 'http://www.adviserinfo.sec.gov/iapd/content/viewform/adv/Sections/iapd_AdvAllPages.aspx' + prevUrl[prevUrl.find('?') : ]
        # print url
        yield FormRequest(
            url,
            # headers = {
            #     "X-Forwarded-For": rand_ip
            # },
            headers = self.headers,
            method="GET",
            meta={
                # 'current_page': 1
                # 'id' : curr_id,
                'companyName' : companyName,
                'zipcode' : zipcode,
                'isEnd' : isEnd
            },
            callback=self.data_parse_viewall,
            errback = self.err_callback
        )

    def data_parse_viewall(self, response):
        # print 'enter view all'
        if response.url == 'http://www.adviserinfo.sec.gov/IAPD/WebLock/WebLock.aspx':
            self.err_handle()
        #-- because the website only running from 11PM to 7AM
        # now = datetime.datetime.now()
        # startTime = now.replace(hour = 3, minute = 30, second = 0, microsecond = 0)
        # endTime = now.replace(hour = 5, minute = 0, second = 0, microsecond = 0)
        # if now <= endTime and now >= startTime:
        #     self.log("sleep for 2 hours", level=log.INFO)
        #     time.sleep(7200)
        #-- end of sleep
        
        # print response.url
        meta = response.request.meta
        # curr_id = meta['id']
        companyName = meta['companyName']
        isEnd = meta['isEnd']
        zipcode = meta['zipcode']
        # print zipcode
        # print "isEnd : " + isEnd
        #save into DB

        #save error page
        # file = open('viewall.html', 'w')
        # file.write(response.body)
        # file.close()
        #finish saving

        #-- find the brochure link and save into DB
        trs = response.xpath("//table[@id='tblBrochures']//table[1]//tr[@class='PrintHistRed']")
        links = []
        for tr in trs:
            href = tr.xpath(".//a[@class='PrintHistRed']/@href").extract()
            links.append(self.base_url + href[0][ href[0].find("javascript: JOpenViewWindow('")+29 : href[0].find("')")])
        
        item = WebscrapingAdviserinfoItem()

        item['datetime'] = '2014-10-23'
        item['name'] = companyName
        item['type'] = 1
        item['link'] = response.url
        item['zipcode'] = zipcode
        yield item

        for link in links:
            item = WebscrapingAdviserinfoItem()

            item['datetime'] = '2014-10-23'
            item['name'] = companyName
            item['type'] = 2
            item['link'] = link
            item['zipcode'] = zipcode
            yield item