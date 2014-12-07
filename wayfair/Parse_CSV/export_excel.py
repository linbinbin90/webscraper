__author__ = 'linbinbin'
import MySQLdb
import xlsxwriter

workbook = xlsxwriter.Workbook('Export.xlsx')
worksheet = workbook.add_worksheet('Summary')
worksheet.write('B1', 'Wayfair')
worksheet.write('E1', 'Amazon')
worksheet.write('A2', 'Item')
worksheet.write('B2', 'Description')
worksheet.write('C2', 'Price')
worksheet.write('D2', 'Link')
worksheet.write('E2', 'Description')
worksheet.write('F2', 'Price')
worksheet.write('G2', 'Link')
db_conn = MySQLdb.connect(
    host    = '127.0.0.1',
    user    = 'root',
    passwd  = '900129lbb',
    db      = 'webscraping_wayfair',
    charset = 'utf8'
)
result_table = '14120215321417552322_result'
conn = db_conn
cursor = conn.cursor()
sql = 'SELECT item_num, description, price, link FROM %s where website="wayfair" order by item_num' % result_table
cursor.execute(sql)
wayfair_records = cursor.fetchall()


row = 2
new_row = 2
col = 0
for item_num, description, price, link in wayfair_records:
    worksheet.write_string(new_row, col, str(item_num))
    worksheet.write_string(new_row, col + 1, description)
    worksheet.write_string(new_row, col + 2, price)
    worksheet.write_string(new_row, col + 3, link)
    new_row += 1

sql = 'SELECT item_num, description, price, link FROM %s where website="amazon" order by item_num' % result_table
cursor.execute(sql)
amazon_records = cursor.fetchall()
col += 3
for item_num, description, price, link in amazon_records:
    worksheet.write_string(row, col + 1, description)
    worksheet.write_string(row, col + 2, price)
    worksheet.write_string(row, col + 3, link)
    row += 1

workbook.close()

