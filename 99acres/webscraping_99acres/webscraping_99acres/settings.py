# -*- coding: utf-8 -*-

# Scrapy settings for webscraping_99acres project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'webscraping_99acres'

SPIDER_MODULES = ['webscraping_99acres.spiders']
NEWSPIDER_MODULE = 'webscraping_99acres.spiders'

# LOG_LEVEL = 'DEBUG'
# LOG_LEVEL = 'WARNING'
LOG_LEVEL = 'INFO'

# COOKIES_DEBUG = True

RETRY_TIMES = 1

DOWNLOAD_DELAY = 3

ITEM_PIPELINES = {
   'webscraping_99acres.pipelines.Webscraping99AcresPipeline': 100
}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'webscraping_99acres (+http://www.yourdomain.com)'
