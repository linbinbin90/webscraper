# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field


class DoorsLowesItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    site      = Field()
    door_type = Field()
    brand     = Field()
    name      = Field()
    price     = Field()
    model     = Field()
    link      = Field()
    total     = Field()
    date      = Field()
