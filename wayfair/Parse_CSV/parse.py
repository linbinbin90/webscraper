__author__ = 'linbinbin'
import csv

def link_parse_wayfair(file):
    links = []
    with open(file, 'rb') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='|')
        for row in spamreader:
            if row[0] != '' and row[0] != 'item':
                # links.append(row[3])
                links.append(row[6])
    return links



print link_parse_wayfair("falconedge_wayfair.csv")