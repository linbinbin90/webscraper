# -*- coding: utf-8 -*-

from scrapy import log
from scrapy.spider import BaseSpider
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.exceptions import CloseSpider
import re
from webscraping_ebay.items import WebscrapingEbayItem

import random
import struct
import socket

class webscraping_EbaySpider(BaseSpider):
    # -- scrapy build in attributes
    name = "webscraping_ebay"
    allowed_domains = ["ebay.com"]
    baseurl = 'http://www.ebay.com/sch/Athletic-/15709/i.html?_from=R40&_nkw=basketball+shoes&rt=nc'
    size = '&US%2520Shoe%2520Size%2520%2528Men%2527s%2529='
    
    def start_requests(self):
        self.log("Scrape initial", level=log.INFO)
        #-- generate the links for every size
        urlMap = {'(None)' : 'http://www.ebay.com/sch/Athletic-/15709/i.html?_from=R40&_nkw=basketball+shoes&rt=nc&US%2520Shoe%2520Size%2520%2528Men%2527s%2529=%21'}
        for i in range(5,16):
        # for i in range(5,6):
            urlMap[str(i)] = self.baseurl + self.size + str(i)
            urlMap[str(i) + '.5'] = self.baseurl + self.size + str(i) + '%252E5'
        # for i in range(16, 22):
        #     urlMap[str(i)] = self.baseurl + self.size + str(i)
        # print urlMap
        for size, url in urlMap.iteritems():
            # rand_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
            yield FormRequest(
                url,
                # headers = {
                #     "X-Forwarded-For": rand_ip
                # },
                method="GET",
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
                    'current_page': 1,
                    'size' : size
                },
                callback=self.data_start_parse
            )

        


    
    def data_start_parse(self, response):
        # if response.status == 200:
        #     self.log("Access data_start_parse success.", level = log.INFO)
        # else:
        #     self.log("Access data_start_parse.", level = log.ERROR)
        #     raise CloseSpider( reason = 'Access fail and stop crawler (%s):%s' % (response.status, response.url) )

        next = '0'
        try:
            meta = response.request.meta
            current_page = meta['current_page']
            size = meta['size']
            self.log("Size: %s ,Page: %s ,Status Code: %s" % (size, current_page, response.status), level=log.INFO)
            # -- save total number

            # -- there have two different style ebay used to show the total number 
            result_total = response.xpath("//span[@class = 'rcnt']/text()").extract()
            if result_total:
            	result_total = str( re.search("[0-9.,]+", str(result_total[0])).group(0) ).replace(',','')
            else:
            	result_total = response.xpath("//span[@class = 'rcnt kwcat']//span[1]/text()").extract()
            	if result_total:
            		result_total = str( re.search("[0-9.,]+", str(result_total[0])).group(0) ).replace(',','')
            	else:
            		result_total = '(None)' # --will change to None later 

            self.log("have %s totally records" % result_total, level=log.INFO) 
            #-- parse the next link, if the last page, it will be 0
            next = response.xpath("//div[@id='PaginationAndExpansionsContainer']//table[@id='Pagination']//td[3]//a[1]/@href").extract()
            # next = response.xpath("//a[@class='pgsel'][contains(concat(' ', @value, ' '), 'Next ')]").extract()
            if next:
	            if('http' in next[0]):
	                next = next[0]
            print next
            #save error page
            # file = open(str(current_page)+'.html', 'w')
            # file.write(response.body)
            # file.close()
            #finish saving

            results_response = response.xpath("//li[@class = 'sresult gvresult']")
            # print len(results_response)
            #-- if response is not NULL, continue to Scrapy until the response is NULL 
            if len(results_response) != 0:
                for result in results_response:
                    #-- get the name data
                    result_name = result.xpath(".//div[@class='gvtitle']//h3[1]//a[1]/text()").extract()
                    # print result_name[0]

                    #-- get the item link
                    result_item_link = result.xpath(".//div[@class='gvtitle']//h3[1]//a[1]/@href").extract()
                    # print result_item_link[0]

                    #-- get bid, buy, best offer
                    result_bid = '(None)' #-- need to change to None later
                    result_buy = '(None)' #-- need to change to None later
                    result_offer = '0' #-- 0 is NO, 1 is YES
                    prices = result.xpath(".//div[contains(concat(' ', @class, ' '), 'price')]")
                    #-- check and get the bid
                    bid = prices.xpath(".//div[@class='bid']")
                    if(len(bid) != 0):
                         tmp_spans = bid.xpath(".//span/text()").extract()
                         result_bid = tmp_spans[len(tmp_spans)-2][1:]
                         # print result_bid

                    #-- check and get the 
                    bin = prices.xpath(".//div[@class='bin']")
                    if(len(bin) != 0):
                        tmp_spans = bin.xpath(".//span/text()").extract()
                        tmp_str = " ".join(tmp_spans).strip().lower()
                        result_buy = str(re.search("[0-9.$]+", tmp_str).group(0))[1:]
                        if('offer' in tmp_str):
                            result_offer = '1'
                    
                    # print result_bid
                    # print result_buy
                    # print result_offer



                    item = WebscrapingEbayItem()

                    item['name']         = result_name
                    item['total']        = result_total
                    item['bid']          = result_bid
                    item['buy']          = result_buy
                    item['offer']        = result_offer
                    item['item_link']    = result_item_link
                    item['page_link']    = response.url
                    item['size']         = size
                    yield item

        except Exception as exp:
            self.log("%s"  % str(exp),  level = log.ERROR)
            #save error page
            file = open(str(current_page)+'.html', 'w')
            file.write(response.body)
            file.close()
            #finish saving

        try:
            if(next == '0'):
                return
            else:
                # url = 'http://www.99acres.com' + str(next)
                # rand_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
                yield FormRequest(
                    next,
                    # headers = {
                    #     "X-Forwarded-For": rand_ip
                    # },
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
                        'current_page': int(current_page) + 1,
                        'size' : size
                    },
                    callback=self.data_start_parse
                )
        except Exception as exp:
            self.log("Don't have next Page or exceed the 10,000 items limit")
