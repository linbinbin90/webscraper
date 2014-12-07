# -*- coding: utf-8 -*-

import socket
import struct
import random



def get_headers():
    dest_ip = socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))
    headers = {
        'Accept'            : 'text/javascript, text/html, application/xml, text/xml, */*',
        'Accept-Encoding'   : 'gzip,deflate',
        'Accept-Language'   : 'zh-TW,zh;q=0.8,en-US;q=0.6,en;q=0.4,ja;q=0.2,zh-CN;q=0.2',
        'Content-type'      : 'application/x-www-form-urlencoded; charset=UTF-8',
        'Connection'        : 'keep-alive',
        'Host'              : 'www.adviserinfo.sec.gov',
        'User-Agent'        : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36',
        'X-Forwarded-For': dest_ip,
        'Client-IP': dest_ip
    }
    return headers