__author__ = 'linbinbin'
import subprocess
import time
import csv
from multiprocessing import Process
import MySQLdb

def getErrList():
    zipcodelist=[]
    db_conn = MySQLdb.connect(
            host='127.0.0.1',
            user='root',
            passwd='900129lbb',
            db='project_adviserinfo_crawling',
            charset='utf8'
        )
    conn = db_conn
    cursor = conn.cursor()
    cursor.execute("""SELECT zipcode FROM err_records""")
    rows = cursor.fetchall()
    for row in rows:
        zipcodelist.append(row[0])
    cursor.execute("""truncate table err_records""")
    conn.commit()
    cursor.close()
    return zipcodelist

def create():
    zipcodes = []
    states = ['"state"', '"CT"', '"DE"', '"PR"', '"VI"', '"NH"', '"ME"', '"RI"', '"VA"', '"VT"', '"WV"', '"AL"', '"NC"', '"SC"', '"MT"', '"PA"', '"MD"', '"WI"', '"IA"', '"MI"', '"IN"', '"OH"', '"KY"', '"TN"', '"AL"', '"PA"', '"NY"']
    # states = ['"DC"', '"NJ"']
    with open('./zipcode.csv', 'rb') as csvfile:
                spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
                for row in spamreader:
                    if row[2] not in states:
                        zipcodes.append(row[0][1:len(row[0])-1])
                    # if row[2] == '"WA"':
                    #     if int(row[0][1:len(row[0])-1]) > 33188:
                    #         zipcodes.append(row[0][1:len(row[0])-1])
                        # print row[0][1:len(row[0])-1]
    #--after finish the scrape, should run getErrList() to make sure no zipcode left(have error when scrape)                
    #zipcodes = getErrList()  
    index = 0
    while(index < len(zipcodes)):
        output = subprocess.check_output("ps -ef | grep scrapy", shell=True)
        proNum = output.count("/usr/local/bin/scrapy crawl webscraping_adviserinfo")
        if proNum < 30:
            # p = Process(target=createScrapy, args=(zipcodes[index],))
            # p.start()
            # p.join()
            stdout_value = subprocess.Popen("scrapy crawl webscraping_adviserinfo -s LOG_FILE=WA.log" + " -a zipcode=" + str(zipcodes[index]) + ' -a DB_table=WA_result', shell=True, stdout=subprocess.PIPE)
            # print '\tstdout: ', repr(stdout_value)
            print 'index=' + str(index) + ' totally: ' + str(len(zipcodes))
            index += 1
        else:
            time.sleep(200)

    print "Finished"


# def createScrapy(zipcode):
#     subprocess.check_output("scrapy crawl webscraping_adviserinfo" + " -a zipcode=" + str(zipcode), shell=True)

if __name__ == '__main__':
    create()