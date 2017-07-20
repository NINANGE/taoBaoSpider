# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymongo
from scrapy.conf import settings
from scrapy.exceptions import DropItem
from scrapy import log


class TaobaoscrapyPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.connectoin = db[settings['MONGODB_COLLECTION']]

    def process_item(self, item, spider):

        valit = True
        for data in item:
            if not data:
                valit = False
                raise DropItem("Missing{0}".format(data))

        if valit:
            self.connectoin.insert(dict(item))
            log.msg("Questing added to MongoDB database!",level=log.DEBUG,spider=spider)
        return item

