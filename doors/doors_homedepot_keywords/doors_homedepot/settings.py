# -*- coding: utf-8 -*-

# Scrapy settings for doors_homedeport project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'doors_homedepot'

SPIDER_MODULES = ['doors_homedepot.spiders']
NEWSPIDER_MODULE = 'doors_homedepot.spiders'

LOG_LEVEL = 'INFO'

RETRY_TIMES = 1

DOWNLOAD_DELAY = 3

ITEM_PIPELINES = {
	'doors_homedepot.pipelines.DoorsHomedepotPipeline': 100
}

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'doors_homedeport (+http://www.yourdomain.com)'
