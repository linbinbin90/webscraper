__author__ = 'linbinbin'
import mysql.connector
import datetime

class DBHandler(object):
    def __init__(self):
        try:
            self.cnx = mysql.connector.connect(user="root", password="900129lbb", host="127.0.0.1", database="Housing", connection_timeout=300)
            # self.cnx = mysql.connector.connect(user="root", password="", host="127.0.0.1", database="Housing", connection_timeout=300)
            self.cursor = self.cnx.cursor()
            self.cur = self.cnx.cursor(buffered=True)
            # print "connect success"
        except Exception as exp:
            print "connect error" + str(exp)

    def GetWebsiteID(self, websiteName):
        try:
            query = ("select Website_id from Website"
                    " where Website_name = %s")
            self.cur.execute(query, (websiteName,))
            for id in self.cur:
                return id[0]
        except Exception as exp:
            print "Get website ID error" + str(exp)

    def GetCityList(self):
        cityList = []
        try:
            query = ("select City_name from City")
            self.cur.execute(query)
            for city_name in self.cur:
                cityList.append(city_name)
        except Exception as exp:
            print "Get city list error: " + str(exp)

    def GetCityID(self, cityName):
        try:
            query = ("select City_id from City"
                    " where City_name = %s")
            self.cur.execute(query, (cityName,))
            for id in self.cur:
                return id[0]
        except Exception as exp:
            print "connect error" + str(exp)

    def Disconnect(self):
        try:
            self.cur.close()
            self.cursor.close()
            self.cnx.close()
        except Exception as exp:
            print "disconnect error" + str(exp)

    def InsertSummary(self, summary):
        try:
            query = ("insert into SearchResultSummary (Website_id, City_id, Type, Result_Num, Time) values(%s, %s, %s, %s, %s)")
            self.cur.execute(query, (summary['Website_id'], summary['City_id'], summary['Type'], summary['Result_Num'], datetime.datetime.now()))
            self.cnx.commit()
            return self.cur._last_insert_id
        except Exception as exp:
            print "insert summary error: " + str(exp)

    def InsertDetail(self, detail):
        try:
            query = ("insert into SearchResultDetail (Summary_id, City_id, Price, Duration, Carpet_area, Builtup_area) values(%s, %s, %s, %s, %s, %s)")
            self.cur.execute(query, (detail['Summary_id'], detail['City_id'], detail['Price'], detail['Duration'], detail['Carpet_area'], detail['Builtup_area']))
            self.cnx.commit()
        except Exception as exp:
            print "insert detail error: " + str(exp)
            print detail

    def InsertLink(self, link):
        try:
            query = ("insert into Link (Link, page) values(%s, %s)")
            self.cur.execute(query, (link['Link'], link['page']))
            self.cnx.commit()
        except Exception as exp:
            print "insert link error: " + str(exp)