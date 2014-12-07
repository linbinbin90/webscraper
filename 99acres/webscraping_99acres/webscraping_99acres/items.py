# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class Webscraping99AcresItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    locality     = Field()
    price        = Field()
    size         = Field()
    date         = Field()
    city         = Field()
    rent_buy_new = Field()
    item_link    = Field()
    city_link    = Field()
    total        = Field()
