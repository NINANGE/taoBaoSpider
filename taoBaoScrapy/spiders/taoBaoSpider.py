#*_ coding:utf-8 _*_
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
import pandas as pd
import re
import time
import requests
import urllib2
import pymongo
import datetime
import json
import copy
from taoBaoScrapy.removeDeleteRepeat import allStoreScrapy

import sys
reload(sys)
sys.setdefaultencoding('utf-8')
df = pd.read_csv('/home/django/nange/taoBaoSpider/taoBaoScrapy/spiders/taoBaoCategory.csv')
# df = pd.read_csv('/Users/zhuoqin/taoBaoScrapy/taoBaoScrapy/spiders/taoBaoCategory.csv')

allPidData = []

class TBSpider(Spider):

    # def __init__(self):
    #     self.allPidData = list()

    name = 'taoBaoSpider'
    allowed_domains = ["taobao.com"]
    start_urls = ['https://taobao.com/']

    def parse(self, response):
        currentTime = datetime.datetime.now().strftime('%Y%m%d')
        times = datetime.datetime.now().strftime('%H')
        results = getAllKeyword()

        for data in results:
            print '是否待开启------------%s'%str(data['state'])
            if (str(data['state'])=='待开启') or ('03' == times):


                nowTime = datetime.datetime.now().strftime('%Y-%m-%d')
                start_Time = datetime.datetime.strptime(nowTime, '%Y-%m-%d')
                end_Time = datetime.datetime.strptime(data['endTime'], '%Y-%m-%d')
                D_value = end_Time - start_Time


                if D_value.days>=0:

                    key = str(data['keyword'])

                    if ' ' in key:
                        key = ''.join(key.split())

                    for i in range(0,int(data['pageNumber'])): #这里不包含101

                        allPidDetailData = []
                        if i==0:
                            try:
                                if len(str(data['priceUpperLimit'])) > 0 or len(str(data['priceDownLimit'])) > 0:

                                    if str(data['market']) == '2':
                                        lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&stats_click=search_radio_all:1&bcoffset=0&js=1&sort=renqi-desc&filter_tianmao=tmall&filter=reserve_price' \
                                                  '[' + str(data['priceUpperLimit']) + ',' + str(data['priceDownLimit']) + ']&q=' + str(key) + '&s=36&initiative_id=staobaoz_' + str(
                                            currentTime) + '&ie=utf8'
                                    else:
                                        # 有最高也有低
                                        lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&stats_click=search_radio_all:1&bcoffset=0&js=1&sort=renqi-desc&filter=reserve_price' \
                                                  '[' + str(data['priceUpperLimit']) + ',' + str(data['priceDownLimit']) + ']&q=' + str(key) + '&s=36&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8'

                                else:
                                    # 第一页最后12个产品
                                    # lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&bcoffset=0&commend=all&sort=renqi-desc&q='+str(key)+'&s=36&initiative_id=tbindexz_'+str(currentTime)+'&ie=utf8'
                                    if str(data['market']) == '2':
                                        lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&bcoffset=0&js=1&sort=renqi-desc&q=' + str(key) + '&ntoffset=4&filter_tianmao=tmall&s=36&initiative_id=staobaoz_' + str(currentTime) + '&ie=utf8'
                                    else:
                                        lastUrl = 'https://s.taobao.com/api?ajax=true&m=customized&bcoffset=0&js=1&sort=renqi-desc&q='+str(key)+'&ntoffset=4&s=36&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8'

                                req = urllib2.Request(lastUrl)
                                res_data = urllib2.urlopen(req)
                                res = res_data.read()
                                babyInfo = json.loads(res)
                                itemList = babyInfo['API.CustomizedApi']['itemlist']['auctions']
                                for j in range(0,len(itemList)):
                                    taoBaoItem = TaobaoscrapyItem()
                                    taoBaoItem['pageNumber'] = i
                                    taoBaoItem['itemID'] = str(data['_id'])
                                    taoBaoItem['customized'] = data['customized']
                                    taoBaoItem['ID'] = itemList[j]['nid']

                                    allPidDetailData.append(itemList[j]['nid'])
                                    if str(data['market']) == '2':
                                        taoBaoItem['detailURL'] = "https://detail.tmall.com/item.htm?id="+ str(itemList[j]['nid'])
                                        taoBaoItem['market'] = '天猫'
                                    else:
                                        if itemList[j]['detail_url'].find('tmall') == -1:
                                            taoBaoItem['detailURL'] = "https://item.taobao.com/item.htm?id=" + str(itemList[j]['nid'])
                                            taoBaoItem['market'] = '淘宝'
                                        else:
                                            taoBaoItem['detailURL'] = "https://detail.tmall.com/item.htm?id=" + str(itemList[j]['nid'])
                                            taoBaoItem['market'] = '天猫'


                                    taoBaoItem['name'] = itemList[j]['raw_title']
                                    taoBaoItem['mainPic'] = 'https:'+itemList[j]['pic_url']
                                    taoBaoItem['price'] = itemList[j]['view_price']


                                    viewSales = str(itemList[j]['view_sales'])
                                    if u'人付款' in viewSales:
                                        # try:
                                        viewSales = viewSales.replace(u'人付款','')
                                        taoBaoItem['payPerson'] = viewSales
                                        # except Exception as e:
                                        #     print e
                                    else:
                                        taoBaoItem['payPerson'] = itemList[j]['view_sales']
                                    taoBaoItem['shopName'] = itemList[j]['nick']
                                    taoBaoItem['categoryId'] = itemList[j]['category']
                                    taoBaoItem['isTmall'] = itemList[j]['isTmall']
                                    taoBaoItem['user_id'] = itemList[j]['user_id']

                                    for k in range(0,len(df)):
                                        if str(df['CategoryId'][k]) == itemList[j]['category']:
                                            taoBaoItem['category'] = str(df['CategoryName'][k])
                                            break
                                        else:
                                            taoBaoItem['category'] = '-'

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

                                    taoBaoItem['categoryTree'] = '-'
                                    taoBaoItem['offTime'] = '-'
                                    taoBaoItem['state'] = str(data['state'])
                                    yield taoBaoItem
                                    # allPidData.append(allPidDetailData)
                            except Exception as e:
                                print '--------------%s'%e


                        #有上限和下限价格的URL
                        if len(str(data['priceUpperLimit']))>0 and len(str(data['priceDownLimit']))>0:
                            if str(data['market']) == '2':
                                url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&ie=utf8&initiative_id=tbindexz_'+str(currentTime)+'&fs=1&filter_tianmao=tmall&sort=renqi-desc' \
                                      '&bcoffset=0&filter=reserve_price%5B'+str(data['priceUpperLimit'])+'%2C'+str(data['priceDownLimit'])+'%5D&s='+str(44*i)
                            else:
                                url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8' \
                                      '&sort=renqi-desc&filter=reserve_price%5B'+str(data['priceUpperLimit'])+'%2C'+str(data['priceDownLimit'])+'%5D&bcoffset=4&ntoffset=4&p4ppushleft=2%2C48&s='+str(44*i)

                        elif len(str(data['priceUpperLimit']))>0 and len(str(data['priceDownLimit']))==0:
                            if str(data['market']) == '2':
                                url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&ie=utf8&initiative_id=tbindexz_'+str(currentTime)+'&fs=1&filter_tianmao=tmall&sort=renqi-desc&' \
                                      'bcoffset=0&filter=reserve_price%5B'+str(data['priceDownLimit'])+'%2C%5D&s='+str(44*i)
                            else:
                                #只有最低，没有最高
                                url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8' \
                                      '&sort=renqi-desc&filter=reserve_price%5B'+str(data['priceUpperLimit'])+'%2C%5D&bcoffset=4&ntoffset=4&p4ppushleft=2%2C48&s='+str(44*i)

                        elif len(str(data['priceUpperLimit'])) ==0 and len(str(data['priceDownLimit']))>0:
                            #只有最高，没有最低
                            if str(data['market']) == '2':
                                url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&ie=utf8&initiative_id=tbindexz_'+str(currentTime)+'&fs=1&filter_tianmao=tmall&sort=renqi-desc&bcoffset=0&' \
                                      'filter=reserve_price%5B%2C'+str(data['priceDownLimit'])+'%5D&s='+str(44*i)
                            else:
                                url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8' \
                                      '&sort=renqi-desc&filter=reserve_price%5B%2C'+str(data['priceDownLimit'])+'%5D&bcoffset=4&ntoffset=4&p4ppushleft=2%2C48&s='+str(44*i)

                        else:
                            #没有最高也没有最低
                            if str(data['market']) == '2':
                                url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&ie=utf8&initiative_id=tbindexz_'+str(currentTime)+'&fs=1&filter_tianmao=tmall&sort=renqi-desc&bcoffset=0&s='+str(44*i)
                            else:
                                url = 'https://s.taobao.com/search?q='+str(key)+'&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_'+str(currentTime)+'&ie=utf8&sort=renqi-desc&bcoffset=4' \
                                      '&ntoffset=4&p4ppushleft=2%2C48&s='+str(44*i)

                        yield Request(url=url,callback=self.page2,meta={'page':i,'productID':str(data['_id']),'market':data['market'],'customized': data['customized'],'state':data['state']})

                    # categoryData(allPidData)
        # print '任务正在进行时---%s' % self.allPidData

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
        isTmall = '"isTmall":(.*?)'
        user_id = '"user_id":"(.*?)"'

        detailURL = '"detail_url":"(.*?)"'



        #
        allPic = re.compile(patPic).findall(body) #图片集合
        allid = re.compile(patid).findall(body)  # 商品Id集合
        allprice = re.compile(patprice).findall(body)  # 商品价格集合
        allName = re.compile(patname).findall(body) #名字集合
        alladdress = re.compile(pataddress).findall(body)  # 商户地址集合
        allPayPerson = re.compile(patPayPerson).findall(body) #全部付款人数集合
        allShopName = re.compile(patShopName).findall(body) #店铺名称
        allCategory = re.compile(category).findall(body)
        allIsTmall = re.compile(isTmall).findall(body)
        allUserId = re.compile(user_id).findall(body)
        allDetailURL = re.compile(detailURL).findall(body)

        allPidData.append(allid)

        for j in range(0,len(allid)):
            taoBaoItem = TaobaoscrapyItem()
            # taoBaoItem['serialNumber'] = j
            taoBaoItem['pageNumber'] = response.meta['page']
            taoBaoItem['itemID'] = response.meta['productID']
            taoBaoItem['customized'] = response.meta['customized']
            taoBaoItem['state'] = response.meta['state']
            taoBaoItem['ID'] = allid[j]
            if str(response.meta['market']) == '2':
                taoBaoItem['detailURL'] = "https://detail.tmall.com/item.htm?id=" + str(allid[j])
                taoBaoItem['market'] = '天猫'
            else:
                if allDetailURL[j].find('tmall') == -1:
                    taoBaoItem['detailURL'] = "https://item.taobao.com/item.htm?id=" + str(allid[j])
                    taoBaoItem['market'] = '淘宝'
                else:
                    taoBaoItem['detailURL'] = "https://detail.tmall.com/item.htm?id=" + str(allid[j])
                    taoBaoItem['market'] = '天猫'

            taoBaoItem['name'] = allName[j]
            taoBaoItem['mainPic'] = 'https:'+allPic[j]
            taoBaoItem['price'] = allprice[j]

            viewSales = str(allPayPerson[j])
            if u'人付款' in viewSales:
                # try:
                viewSales = viewSales.replace(u'人付款', '')
                taoBaoItem['payPerson'] = viewSales
                # except Exception as e:
                #     print e
            else:
                taoBaoItem['payPerson'] = allPayPerson[j]

            taoBaoItem['shopName'] = allShopName[j]
            taoBaoItem['categoryId'] = allCategory[j]
            taoBaoItem['isTmall'] = allIsTmall[j]
            taoBaoItem['user_id'] = allUserId[j]

            for k in range(0, len(df)):
                if str(df['CategoryId'][k]) == allCategory[j]:
                    taoBaoItem['category'] = str(df['CategoryName'][k])
                    break
                else:
                    taoBaoItem['category'] = '-'

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
            taoBaoItem['categoryTree'] = '-'
            taoBaoItem['offTime'] = '-'

            yield taoBaoItem

        # print '测试一下---%s' % self.allPidData
        # categoryData()


        #数组转字符串

    def close(spider, reason):

        try:
            time.sleep(1.0)
            print '任务正在进行时---%s' % allPidData
            result = getTaoBaoData()
            categoryData(allPidData,result)

            allStoreScrapy()
        except Exception as e:
            print e
        finally:
            print '到些结束啦'





#mongodb连接类
class mongodbConn:
    conn = None
    servers = "mongodb://192.168.3.172:27017"
    # servers = "mongodb://127.0.0.1:27017"
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

def getTaoBaoData():
    dbconn = mongodbConn()
    dbconn.connect()
    conn = dbconn.getConn()

    table = conn.TaoBaoScrapyDB.TaoBaoSTB

    result = table.find({})
    return result




def categoryData(allPidData,results):
    resultsList = list(results)
    for categoryPidData in allPidData:
        # print '数据---------%s'%allPidData

        if len(categoryPidData)==0:
            continue
        
        str_data = ",".join(categoryPidData)
        # print type(allPidData)
        #
        url = 'http://plugin.qly360.com/productDetailList.do?spid=' + str_data

        print '地址---------%s' % url

        req = urllib2.Request(url)
        res_data = urllib2.urlopen(req)
        res = res_data.read()

        babyCategoryInfo = json.loads(res)
        # result = copy.deepcopy(results)
        # result = getTaoBaoData()
        for data in babyCategoryInfo:
            ID = data['pid']

            # try:
            if '-' in data['category']:
                # 类目
                category = data['category'].encode("utf-8")
                categoryTree = category
                categoryName = category.split('-')[-1]

            else:
                categoryTree = data['category'].encode("utf-8")
                categoryName = '-'

                # 下架时间
            time_local = data['delist'] / 1000

            time_local = time.localtime(time_local)

            offTime = time.strftime("%Y-%m-%d %H:%M:%S", time_local)
            product = {
                'category': categoryName,
                'categoryTree': categoryTree,
                'offTime': offTime,
            }

            # result = getTaoBaoData()
            for categoryData in resultsList:
                # print '结果数据源----------%s----%s---%s---%s'%(result.count(),categoryName,categoryData['ID'],ID)
                if int(categoryData['ID']) == ID:
                    print '相等'
                    saveCategory(categoryData['ID'], categoryName, categoryTree, offTime)


def saveCategory(ID,categoryName,categoryTree,offTime):
    dbconn = mongodbConn()
    dbconn.connect()
    conn = dbconn.getConn()
    table = conn.TaoBaoScrapyDB.TaoBaoSTB  # 获取数据库中的所有关键字条件

    try:
        table.update({'ID': ID}, {'$set': {'category': categoryName}},multi=True)
        table.update({'ID': ID}, {'$set': {'categoryName': categoryTree}},multi=True)
        table.update({'ID': ID}, {'$set': {'offTime': offTime}},multi=True)
        # if table.insert(result):
        print '保存数据成功--%s--%s--%s--%s'%(ID,categoryTree,categoryName,offTime)
        # print '数据-----%s'%(table.update({'ID': ID}, {'$set': {'offTime': offTime}}))
    except Exception as e:
        print('保存到MONGODB失败', e)















