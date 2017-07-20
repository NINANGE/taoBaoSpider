#_*_ coding:utf-8 _*_
# https://s.taobao.com/search?q=%E5%AE%9E%E6%9C%A8%E5%BA%8A&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.50862.201856-taobao-item.1
# &ie=utf8&initiative_id=tbindexz_20170712&bcoffset=4&ntoffset=4&p4ppushleft=1%2C48&sort=renqi-desc
# https://s.taobao.com/search?q=%E5%AE%9E%E6%9C%A8%E5%BA%8A&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.50862.201856-taobao-item.1
# &ie=utf8&initiative_id=tbindexz_20170712&bcoffset=4&ntoffset=4&p4ppushleft=1%2C48&sort=renqi-desc&s=44
# https://s.taobao.com/search?q=%E5%AE%9E%E6%9C%A8%E5%BA%8A&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.50862.201856-taobao-item.1
# &ie=utf8&initiative_id=tbindexz_20170712&bcoffset=4&ntoffset=4&p4ppushleft=1%2C48&sort=renqi-desc&s=88

# https://s.taobao.com/search?q=%E5%AE%9E%E6%9C%A8%E5%BA%8A&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.50862.201856-taobao-item.1
# &ie=utf8&initiative_id=tbindexz_20170717&sort=renqi-desc&bcoffset=4&ntoffset=4&p4ppushleft=1%2C48&s=44

# https://s.taobao.com/search?q=%E5%AE%9E%E6%9C%A8%E5%BA%8A&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.50862.201856-taobao-item.1
# &ie=utf8&initiative_id=tbindexz_20170717&sort=renqi-desc&bcoffset=4&ntoffset=4&p4ppushleft=1%2C48&filter=reserve_price%5B1000%2C10000%5D&s=44

# https://s.taobao.com/search?q=%E5%AE%9E%E6%9C%A8%E5%BA%8A&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.50862.201856-taobao-item.1
# &ie=utf8&initiative_id=tbindexz_20170717&sort=renqi-desc&bcoffset=4&ntoffset=4&p4ppushleft=1%2C48&filter=reserve_price%5B1000%2C10000%5D&s=88

#有最低价格没有最高
# https://s.taobao.com/search?q=%E5%AE%9E%E6%9C%A8%E5%BA%8A&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.50862.201856-taobao-item.1
# &ie=utf8&initiative_id=tbindexz_20170717&sort=renqi-desc&bcoffset=4&ntoffset=4&p4ppushleft=1%2C48&filter=reserve_price%5B600%2C%5D&s=44

#有最高价没有最低
# https://s.taobao.com/search?q=%E5%AE%9E%E6%9C%A8%E5%BA%8A&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.50862.201856-taobao-item.1
# &ie=utf8&initiative_id=tbindexz_20170717&sort=renqi-desc&bcoffset=4&ntoffset=4&p4ppushleft=1%2C48&filter=reserve_price%5B%2C56.3%5D

#最高和最低都有
# https://s.taobao.com/search?q=%E5%AE%9E%E6%9C%A8%E5%BA%8A&imgfile=&commend=all&ssid=s5-e&search_type=item&sourceId=tb.index&spm=a21bo.50862.201856-taobao-item.1
# &ie=utf8&initiative_id=tbindexz_20170717&sort=renqi-desc&bcoffset=4&ntoffset=4&p4ppushleft=1%2C48&filter=reserve_price%5B600%2C2500%5D


# from scrapy.spider import Spider
from scrapy.spiders import Spider
from scrapy import Selector
from scrapy import Request
from taoBaoScrapy.items import TaobaoscrapyItem
import re
import time
import requests
import urllib2
import pymongo
import datetime
import json

import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class TBSpider(Spider):
    name = 'taoBaoSpider'
    allowed_domains = ["taobao.com"]
    start_urls = ['https://taobao.com/']

    def parse(self, response):
        currentTime = datetime.datetime.now().strftime('%Y%m%d')
        results = getAllKeyword()

        for data in results:

            nowTime = datetime.datetime.now().strftime('%Y-%m-%d')
            start_Time = datetime.datetime.strptime(nowTime, '%Y-%m-%d')
            end_Time = datetime.datetime.strptime(data['endTime'], '%Y-%m-%d')
            D_value = end_Time - start_Time


            if D_value.days>=0:

                key = str(data['keyword'])

                if ' ' in key:
                    key = ''.join(key.split())

                for i in range(0,int(data['pageNumber'])): #这里不包含101

                    if i==0:

                        if len(str(data['priceUpperLimit'])) > 0 or len(str(data['priceDownLimit'])) > 0:
                            # 有最高也有低
                            lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&stats_click=search_radio_all:1&bcoffset=0&js=1&sort=renqi-desc&filter=reserve_price' \
                                      '[' + str(data['priceUpperLimit']) + ',' + str(data['priceDownLimit']) + ']&q=' + str(key) + '&s=36&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8'

                            #有最低，没最高
                            # lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&stats_click=search_radio_all:1&bcoffset=0&js=1&sort=renqi-desc&filter=reserve_price' \
                            #           '[' + str(data['priceUpperLimit']) + ',]&q='+str(key)+'&s=36&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8'
                            #有最高，没最低
                            # lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&stats_click=search_radio_all:1&bcoffset=0&js=1&sort=renqi-desc&filter=reserve_price' \
                            #           '[,' + str(data['priceDownLimit']) + ']&q='+str(key)+'&s=36&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8'

                        else:
                            # 第一页最后12个产品
                            # lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&bcoffset=0&commend=all&sort=renqi-desc&q='+str(key)+'&s=36&initiative_id=tbindexz_'+str(currentTime)+'&ie=utf8'

                            lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&bcoffset=0&js=1&sort=renqi-desc&q='+str(currentTime)+'&ntoffset=4&s=36&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8'

                        req = urllib2.Request(lastUrl)
                        res_data = urllib2.urlopen(req)
                        res = res = res_data.read()
                        babyInfo = json.loads(res)
                        itemList = babyInfo['API.CustomizedApi']['itemlist']['auctions']
                        for j in range(0,len(itemList)):
                            taoBaoItem = TaobaoscrapyItem()
                            taoBaoItem['pageNumber'] = i
                            taoBaoItem['itemID'] = str(data['_id'])
                            taoBaoItem['ID'] = itemList[j]['nid']
                            taoBaoItem['detailURL'] = "https://item.taobao.com/item.htm?id=" + str(itemList[j]['nid'])

                            taoBaoItem['name'] = itemList[j]['raw_title']
                            taoBaoItem['mainPic'] = 'https:'+itemList[j]['pic_url']
                            taoBaoItem['price'] = itemList[j]['view_price']
                            taoBaoItem['payPerson'] = itemList[j]['view_sales']
                            taoBaoItem['shopName'] = itemList[j]['nick']
                            taoBaoItem['category'] = itemList[j]['category']

                            provString = ''
                            cityStr = ''
                            if ' ' in itemList[j]['item_loc'] and len(itemList[j]['item_loc']) > 0:
                                alladdressData = itemList[j]['item_loc'].split(' ')
                                provString = alladdressData[0]
                                cityStr = alladdressData[1]
                            else:
                                provString = ' '
                                cityStr = itemList[j]['item_loc']

                            taoBaoItem['province'] = provString
                            taoBaoItem['city'] = cityStr

                            taoBaoItem['year'] = time.strftime('%Y', time.localtime(time.time()))
                            taoBaoItem['month'] = time.strftime('%m', time.localtime(time.time()))
                            taoBaoItem['yearAndMonth'] = time.strftime('%Y-%m-%d', time.localtime(time.time()))


                            yield taoBaoItem



                    #有上限和下限价格的URL
                    if len(str(data['priceUpperLimit']))>0 and len(str(data['priceDownLimit']))>0:
                        url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8' \
                              '&sort=renqi-desc&filter=reserve_price%5B'+str(data['priceUpperLimit'])+'%2C'+str(data['priceDownLimit'])+'%5D&bcoffset=4&ntoffset=4&p4ppushleft=2%2C48&s='+str(44*i)

                    elif len(str(data['priceUpperLimit']))>0 and len(str(data['priceDownLimit']))==0:

                        #只有最低，没有最高
                        url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8' \
                              '&sort=renqi-desc&filter=reserve_price%5B'+str(data['priceUpperLimit'])+'%2C%5D&bcoffset=4&ntoffset=4&p4ppushleft=2%2C48&s='+str(44*i)

                    elif len(str(data['priceUpperLimit'])) ==0 and len(str(data['priceDownLimit']))>0:
                        #只有最高，没有最低
                        url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8' \
                              '&sort=renqi-desc&filter=reserve_price%5B%2C'+str(data['priceDownLimit'])+'%5D&bcoffset=4&ntoffset=4&p4ppushleft=2%2C48&s='+str(44*i)

                    else:
                        #没有最高也没有最低
                        url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8&sort=renqi-desc&bcoffset=4' \
                              '&ntoffset=4&p4ppushleft=2%2C48&s='+str(44*i)

                    yield Request(url=url,callback=self.page2,meta={'page':i,'productID':str(data['_id'])})



    def page2(self,response):
        body = response.body.decode("utf-8", "ignore")
        patPic = '"pic_url":"(.*?)"'
        patid = '"nid":"(.*?)"'
        patprice = '"view_price":"(.*?)"'
        patname = '"raw_title":"(.*?)"'
        patPayPerson = '"view_sales":"(.*?)"'

        pataddress = '"item_loc":"(.*?)"'
        patShopName = '"nick":"(.*?)"'
        category = '"category":"(.*?)"'

        allPic = re.compile(patPic).findall(body) #图片集合
        allid = re.compile(patid).findall(body)  # 商品Id集合
        allprice = re.compile(patprice).findall(body)  # 商品价格集合
        allName = re.compile(patname).findall(body) #名字集合
        alladdress = re.compile(pataddress).findall(body)  # 商户地址集合
        allPayPerson = re.compile(patPayPerson).findall(body) #全部付款人数集合
        allShopName = re.compile(patShopName).findall(body) #店铺名称
        allCategory = re.compile(category).findall(body)

        for j in range(0,len(allid)):
            taoBaoItem = TaobaoscrapyItem()
            # taoBaoItem['serialNumber'] = j
            taoBaoItem['pageNumber'] = response.meta['page']
            taoBaoItem['itemID'] = response.meta['productID']
            taoBaoItem['ID'] = allid[j]
            taoBaoItem['detailURL'] = "https://item.taobao.com/item.htm?id=" + str(allid[j])

            taoBaoItem['name'] = allName[j]
            taoBaoItem['mainPic'] = 'https:'+allPic[j]
            taoBaoItem['price'] = allprice[j]
            taoBaoItem['payPerson'] = allPayPerson[j]
            taoBaoItem['shopName'] = allShopName[j]
            taoBaoItem['category'] = allCategory[j]

            provString = ''
            cityStr = ''
            if ' ' in alladdress[j] and len(alladdress[j])>0:
                alladdressData = alladdress[j].split(' ')
                provString = alladdressData[0]
                cityStr = alladdressData[1]
            else:
                provString=' '
                cityStr = alladdress[j]

            taoBaoItem['province'] = provString
            taoBaoItem['city'] = cityStr


            taoBaoItem['year'] = time.strftime('%Y',time.localtime(time.time()))
            taoBaoItem['month'] = time.strftime('%m',time.localtime(time.time()))
            taoBaoItem['yearAndMonth'] = time.strftime('%Y-%m-%d',time.localtime(time.time()))

            yield taoBaoItem



#mongodb连接类
class mongodbConn:
    conn = None
    servers = "mongodb://192.168.3.172:27017"

    def connect(self):
        self.conn = pymongo.MongoClient(self.servers)
    def close(self):
        return self.conn.disconnect()
    def getConn(self):
        return self.conn

def getAllKeyword():
    dbconn = mongodbConn()
    dbconn.connect()
    conn = dbconn.getConn()

    table = conn.TaoBaoScrapyDB.projectKeyWordTB #获取数据库中的所有关键字条件

    result = table.find({})
    return result





















