# -*- coding: utf-8 -*-

# Scrapy settings for webscraping_wayfair project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'webscraping_wayfair'

SPIDER_MODULES = ['webscraping_wayfair.spiders']
NEWSPIDER_MODULE = 'webscraping_wayfair.spiders'

LOG_LEVEL = 'INFO'

RETRY_TIMES = 1

DOWNLOAD_DELAY = 3

ITEM_PIPELINES = {
	'webscraping_wayfair.pipelines.WebscrapingWayfairPipeline': 100
}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'webscraping_wayfair (+http://www.yourdomain.com)'
