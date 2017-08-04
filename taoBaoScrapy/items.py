# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class TaobaoscrapyItem(scrapy.Item):
    serialNumber = scrapy.Field()
    pageNumber = scrapy.Field()
    itemID = scrapy.Field()
    ID = scrapy.Field()
    name = scrapy.Field()
    mainPic = scrapy.Field()
    price = scrapy.Field()
    payPerson = scrapy.Field()
    province = scrapy.Field()
    city = scrapy.Field()
    shopName = scrapy.Field()
    year = scrapy.Field()
    month = scrapy.Field()
    yearAndMonth = scrapy.Field()
    detailURL = scrapy.Field()
    categoryId = scrapy.Field()
    category = scrapy.Field()
    isTmall = scrapy.Field()
    user_id = scrapy.Field()
    market = scrapy.Field()
    customized = scrapy.Field()
    categoryTree = scrapy.Field()
    offTime = scrapy.Field()
    state = scrapy.Field()







