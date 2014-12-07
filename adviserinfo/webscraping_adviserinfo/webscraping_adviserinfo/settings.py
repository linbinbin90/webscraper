# -*- coding: utf-8 -*-

# Scrapy settings for webscraping_adviserinfo project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'webscraping_adviserinfo'

SPIDER_MODULES = ['webscraping_adviserinfo.spiders']
NEWSPIDER_MODULE = 'webscraping_adviserinfo.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'webscraping_adviserinfo (+http://www.yourdomain.com)'
LOG_LEVEL = 'INFO'

RETRY_TIMES = 1

DOWNLOAD_DELAY = 3

ITEM_PIPELINES = {
	'webscraping_adviserinfo.pipelines.WebscrapingAdviserinfoPipeline': 100
}