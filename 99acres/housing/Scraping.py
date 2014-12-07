__author__ = 'linbinbin'
# -*- coding: utf-8 -*-
import logging
import DB
from bs4 import BeautifulSoup
import socket
import struct
import random
import urllib2
import urllib
import time
import mechanize
import datetime
import threading

#-- initial the log format
def init_logging():
    log_format = '[%(asctime)s][%(module)s][%(levelname)s][%(threadName)s][%(funcName)s:%(lineno)d]: %(message)s'
    logging.basicConfig(format=log_format, level=logging.DEBUG, filename='99acer.log', filemode='w')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger("").addHandler(console_handler)

    logging.info("Initialize OK, logging start")


#-- input: string url, output: urllib2 urlopen result
def browse(url):
    dest_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.131 Safari/537.36',
        'Referer': 'http://www.99acres.com/',
        'X-Forwarded-For': dest_ip,
    }
    my_request = urllib2.Request(url, headers=headers)
    start_ts = time.time()
    try:
        my_response = urllib2.urlopen(my_request, timeout=360)
        my_page = my_response.read()
    except Exception as exp:
        print url + " have problem: " + str(exp)
        logging.debug(url + " have problem: " + str(exp))
        return None
    end_ts = time.time()
    # logging.info("Download URL: %s, Cost %.2f Sec", url, end_ts - start_ts)


    return my_page


#-- convert urllib2.urlopen().read() to a beautifulsoup instance
def Convert2Soup(content):
    start_ts = time.time()
    soup = BeautifulSoup(content)
    end_ts = time.time()
    # logging.debug('Get Beautiful Soup Cost: %.2f sec', end_ts - start_ts)
    return soup

#-- calculate the duration days based on the posttime and current time, using datetime.date
def GetDuration(postTime):
    months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'June': 6, 'Jun': 6, 'July': 7, 'Jul': 7, 'Aug': 8,
              'Sep': 9, 'Oct': 10,
              'Nov': 11, 'Dec': 12}
    now = datetime.datetime.now().strftime('%Y-%m-%d')
    tmp = now.split('-')
    d1 = datetime.date(int(tmp[0]), int(tmp[1]), int(tmp[2]))

    tmp = postTime.split(',')
    year = int(tmp[1])
    tmp = tmp[0].split()
    month = months[tmp[0]]
    day = tmp[1]
    d2 = datetime.date(int(year), int(month), int(day))
    delta = d1 - d2
    return delta.days

#-- baseURL: website link
#-- url: partial url, the full url should be baseURL+url
#-- type:  B, R, P
#-- this function is for getting the total link for one city, and then parse every link by multi-thread
def SearchCity(baseURL, url, websiteName, cityName, type):
    start_ts = time.time()
    links = []
    db = None
    try:
        db = DB.DBHandler()
        websiteID = db.GetWebsiteID(websiteName)
        cityID = db.GetCityID(cityName)
        #-- first time open a city search page, get the search result and how many pages does this result have
        myPage = browse(baseURL + url)
        count = 0;
        while (myPage == None):
            myPage = browse(baseURL + url)
            count += 1
            if (count >= 3):
                logging.info(url + " Can not access, skip!!!")
                return
        soup = Convert2Soup(myPage)
        #-- total search result
        result_num = soup.find('div', {'class': 'sRTabs'}).find('a', {'id': 'qs_link'}).text.split()
        summary = {'Website_id': websiteID, 'City_id': cityID, 'Type': type, 'Result_Num': result_num[0]}
        summaryID = db.InsertSummary(summary)
        #-- total page for this search
        pageNum = soup.findAll('a', {'class': 'pgsel'})
        maxPage = 0;
        #-- inorder to get the page number of this city
        for i in range(0, pageNum.__len__() - 1):
            if ("Next" not in pageNum[i]['value']):
                maxPage = max(maxPage, int(pageNum[i]['value']))
        logging.debug(url + '  have ' + str(maxPage) + '  pages')
        #-- open every page and get the links related to this city
        for page in range(1, maxPage+1):
            breakSign = False
            count = 0
            myPage1 = browse(baseURL + url + '-page-' + str(page))
            while (myPage1 == None):
                myPage1 = browse(baseURL + url + '-page-' + str(page))
                count += 1
                if (count >= 3):
                    logging.info(baseURL + url + '-page-' + str(page) + " Can not access, skip!!!")
                    breakSign = True
                    break
            if(breakSign):
                continue
            soup1 = Convert2Soup(myPage1)
            properties = soup1.findAll('div', {'class': 'srpWrap'})
            for i in range(0, properties.__len__()):
                #--below is for get the onclick openPropDescpage link
                start = properties[i]['onclick'].find('openPropDescPage') + 18
                end = properties[i]['onclick'].find("','", start)
                onclick = properties[i]['onclick'][start: end]
                links.append(baseURL + onclick)

            logging.debug(url + '  finish page: ' + str(page))
        # print links.__len__()
        db.Disconnect()
        print(cityName + ' finished parsing the link, totally: ' + str(links.__len__()))

        # --multithread
        # --for every link, scrape the area size, price, post time
        threads = []

        for link in links:
            thread = ThreadLinksHandler(link, summaryID, cityID)
            thread.start()
            threads.append(thread)

            print("Current threading number is: %s", len(threading.enumerate()))
            while True:
                if len(threading.enumerate()) < 120:
                    break
                else:
                    print("Thread number > 80, sleep a while")
                    time.sleep(100)

        for thread in threads:
            thread.join()
    except Exception as exp:
        print exp
        logging.debug(str(cityName) + '  have problem!!! ' + str(exp))
        if(db != None):
            db.Disconnect()
    end_ts = time.time()
    logging.info("Parse city: %s, Cost %.2f Sec", url, end_ts - start_ts)


def main():
    print "start running"

    baseURL = 'http://www.99acres.com'
    cityName = [ 'Kolkata', 'Chennai','Bangalore','Hyderabad','Ahmedabad','Pune','Surat','Jaipur', 'Mumbai','New Delhi']
    url4sale = ['/property-in-kolkata-ffid','/property-in-chennai-ffid','/property-in-bangalore-ffid','/property-in-hyderabad-ffid','/property-in-ahmedabad-ffid','/property-in-pune-ffid','/property-in-surat-ffid','/property-in-jaipur-ffid', '/property-in-mumbai-ffid','/property-in-delhi-ncr-ffid']
    url4rent = ['/rent-property-in-kolkata-ffid','/rent-property-in-chennai-ffid','/rent-property-in-bangalore-ffid', '/rent-property-in-hyderabad-ffid', '/rent-property-in-ahmedabad-ffid', '/rent-property-in-pune-ffid', '/rent-property-in-surat-ffid', '/rent-property-in-jaipur-ffid', '/rent-property-in-mumbai-ffid', '/rent-property-in-delhi-ncr-ffid']
    for index in range(0, url4sale.__len__()):
        SearchCity(baseURL, url4sale[index], '99aceres', cityName[index], 'B')

    for index in range(0, url4rent.__len__()):
        SearchCity(baseURL, url4rent[index], '99aceres', cityName[index], 'R')

    #--option for multithread  for 10 city srape in the same time
    # threads = []
    #
    # count = 0
    # for index in range(0, url4sale.__len__()):
    #     thread = ThreadHandler(baseURL, url4sale[index], '99aceres', cityName[index], 'B')
    #     thread.start()
    #     threads.append(thread)
    #
    # for thread in threads:
    #     thread.join()

#-- thread handler for searchCity function, it's optional, since it's better to use single thread for searchCity
class ThreadHandler(threading.Thread):
    def __init__(self, baseURL, url, websiteName, cityName, type):
        self.baseURL = baseURL
        self.url = url
        self.websiteName = websiteName
        self.cityName = cityName
        self.type = type
        threading.Thread.__init__(self)

    def run(self):
        SearchCity(self.baseURL, self.url, self.websiteName, self.cityName, self.type)

#-- thread handler for searchByLink function
class ThreadLinksHandler(threading.Thread):
    def __init__(self, link, summaryID, cityID):
        self.link = link
        self.summaryID = summaryID
        self.cityID = cityID
        threading.Thread.__init__(self)

    def run(self):
        searchByLink(self.link, self.summaryID, self.cityID)

#-- every link will be a thread, it will parse the price, area, duration and store it into searchResultDetail
def searchByLink(link, summaryID, cityID):
    start_link = time.time()
    db = None
    try:
        count = 0
        link = '%20'.join(link.split())
        myPage2 = browse(link)
        while (myPage2 == None):
            myPage2 = browse(link)
            count += 1
            if (count >= 3):
                logging.info(link + " Can not access, skip!!!")
                return
        soup2 = Convert2Soup(myPage2)
        price = soup2.find('span', {'class': 'redPd b'})
        try:
            if price:
                if ('on Request' in price.text):
                    price = price.text
                else:
                    price = price.text[2:]
            else:
                price = '0'
        except Exception as exp:
            logging.debug(link + "have weried price %s , regard as 0", price.text)
            price = '0'
        carpetArea = soup2.find('b', {'id': 'carpetArea_span'})
        if carpetArea:
            carpetArea = carpetArea.text[2:]
        else:
            carpetArea = '0'
        builtupArea = soup2.find('i', {'id': 'builtupArea_span'})
        if builtupArea:
            builtupArea = builtupArea.text[2:]
        else:
            builtupArea = '0'
        postTime = soup2.find('span', {'class': 'PostdByPd'})
        if postTime:
            postTime = postTime.text[11:]
        else:
            postTime = '0'
        duration = GetDuration(postTime)
        detail = {'Summary_id': summaryID, 'City_id': cityID, 'Price': price, 'Duration': duration,
                  'Carpet_area': carpetArea, 'Builtup_area': builtupArea}
        db = DB.DBHandler()
        db.InsertDetail(detail)
        db.Disconnect()
    except Exception as exp:
        if(db != None):#-- make sure the connection will close even there have error when parse the data
            db.Disconnect()
        logging.debug(link + '  have problem!!!!  ' + str(exp))

    end_link = time.time()
    print("Parse URL: %s, Cost %.2f Sec", link, end_link - start_link)


if __name__ == '__main__':
    init_logging()
    main()

