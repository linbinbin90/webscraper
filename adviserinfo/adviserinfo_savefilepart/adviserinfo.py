import re
import mmap
import MySQLdb
import urllib2
import cookielib
from pattern.web import URL


keyword_list = []
name_list = []
save_file_path = 'data/'
db = MySQLdb.connect(unix_socket = '/Applications/MAMP/tmp/mysql/mysql.sock',host='localhost', user='root',passwd='root', db='adviserinfo')

def get_keyword():
	cursor = db.cursor()
	cursor.execute("SELECT * FROM keyword")
	for row in cursor.fetchall():
		keyword_list.append(row[1])
	cursor.close()

def get_name():
	cursor = db.cursor()
	cursor.execute("SELECT name FROM adviserinfo GROUP BY name")
	for row in cursor.fetchall():
		name_list.append(row[0])
	cursor.close()		

def get_data():
	cursor = db.cursor()
	for name in name_list:
		cursor.execute("SELECT * FROM adviserinfo WHERE name = '%s' GROUP BY link" % (name) )
		for row in cursor.fetchall():
			has_keyword = search_keyword(row[0], row[4])
			if has_keyword:
				save_file(name)
				break
	cursor.close()

def get_result_by_name(name):
	link_list = []
	cursor = db.cursor()
	cursor.execute("SELECT * FROM adviserinfo WHERE name = '%s' GROUP BY link" % (name) )
	for row in cursor.fetchall():
		row_list = [row[0],row[3],row[4]]
		link_list.append(row_list)
	cursor.close()
	return link_list

def search_keyword(id, link):
 	print "id:" + str(id)
 	fileurl = link
	cj = cookielib.CookieJar()
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
	
	# print("... Sending HTTP GET to %s" % theurl)
	try:
		request = urllib2.Request(fileurl)
		f = opener.open(request)
		data = f.read()
		for keyword in keyword_list:
			search = re.findall(keyword, data)
			if search:
				print 'found',keyword,len(search)
				return True
		f.close()
	except Exception, e:
		print fileurl
		# else:
			# print 'not found'
	
	opener.close()
	return False

def save_file(name):
	result_list = get_result_by_name(name)
	for result in result_list:
		id 	 = result[0]
		type = result[1]
		link = result[2]

		url = URL(link)
		if type == 1:
			extension = '.html'
		else:
			extension = '.pdf'

		# file_name = str(id) + "_" + name + extension
		file_name = name + extension
		FILE = open('data/' + file_name, "wb")
		FILE.write(url.download(cached=False))
		FILE.close()

def start():
	get_keyword()
	get_name()
	get_data()
	# print keyword_list
start()
