# -*- coding: utf-8 -*-


import urllib2
import pymongo
import time
from memoryRelease import memoryReleases
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


def allStoreScrapy():
    crate_temTable() #创建临时表，去重

    result = getAllId()
    j = 0
    for data in result:
        j += 1
        # print 'j-----------————%s'%j
        if data['customized'] == '是':

            babyID = data['ID']

            for k in range(1,11): #以索菲亚为基准

                url = 'https://map.taobao.com/item/api/itemStoreList.do?locType=current&_input_charset=utf-8&source=map_itemdetail&pageSize=200&pageNo='+str(k)+'&itemId=' + str(babyID)

            # url = 'https://map.taobao.com/item/api/itemStoreList.do?locType=current&_input_charset=utf-8&source=map_itemdetail&pageSize=20000&itemId=' + str(babyID)

                print 'Address---%s' % url
                req = urllib2.Request(url)
                dataResult = urllib2.urlopen(req)
                res = eval(dataResult.read())

                try:
                    storeInfo = res['stores']
                    if len(storeInfo) > 0:
                        try:
                            for i in range(0, len(storeInfo)):

                                product = {
                                    'longitude': storeInfo[i]['posx'],
                                    'latitude': storeInfo[i]['posy'],
                                    'address': str(storeInfo[i]['address']).decode('unicode_escape'),
                                    'cityCode': storeInfo[i]['city'],
                                    'ID': storeInfo[i]['id'],
                                    'storeName': str(storeInfo[i]['name']).decode('unicode_escape'),
                                    'tel': storeInfo[i]['tel'],
                                    'url': 'https:' + str(storeInfo[i]['url']),
                                    'district': storeInfo[i]['district'],
                                    'itemID': str(data['itemID'])
                                }
                                save_to_mongodb(product)
                        except Exception as e:
                            print 'error-------%s' % e
                    else:
                        break
                except Exception as e:
                    print 'Error in parse 。。。%s'%e



        # if j == result.count():
        #     print '进来了------%s' % j
    memoryReleases() #释放内存
    conneStore()
            # return




#mongodb连接类
class mongodbConn:
    conn = None
    servers = "mongodb://192.168.3.172:27017"
    # servers = "mongodb://127.0.0.1:27017"
    def connect(self):
        self.conn = pymongo.MongoClient(self.servers)
    def close(self):
        return self.conn.close()
    def getConn(self):
        return self.conn

def getAllId():
    # conn = pymongo.MongoClient('localhost',27017)
    # table = conn.TaoBaoScrapyDB.allStoreResults

    dbconn = mongodbConn()
    dbconn.connect()
    conn = dbconn.getConn()
    # table = conn.TaoBaoScrapyDB.allStoreResults
    # result = table.find({})

    return conn.TaoBaoScrapyDB.allStoreResults.find({})

def conneStore():

    dbconn = mongodbConn()
    dbconn.connect()
    conn = dbconn.getConn()
    table = conn.TaoBaoScrapyDB.ALLStoreTB  # 获取数据库中的所有关键字条件

    try:
        for url in table.distinct('address'):  # 使用distinct方法，获取每一个独特的元素列表
            num = table.count({"address": url})  # 统计每一个元素的数量
            print num
            for i in range(1, num):  # 根据每一个元素的数量进行删除操作，当前元素只有一个就不再删除
                print 'delete %s %d times ' % (url, i)
                # 注意后面的参数， 很奇怪，在mongo命令行下，它为1时，是删除一个元素，这里却是为0时删除一个
                table.remove({"address": url}, 0)
            # for i in table.find({"longitude": url}):  # 打印当前所有元素
            #     print i
        print '去重成功***'
    except Exception as e:
        print '2Error in parse 。。。%s' % e
    finally:
        # dbconn.close()
        # dbconn = None
        memoryReleases()  # 释放内存
        print 'finish******1'

#保存到mongodb数据库中
def save_to_mongodb(result):
    # dbconn = mongodbConn()
    # dbconn.connect()
    # conn = dbconn.getConn()
    # table = conn.TaoBaoScrapyDB.ALLStoreTB  # 获取数据库中的所有关键字条件

    conn = pymongo.MongoClient('192.168.3.172', 27017)
    # conn = pymongo.MongoClient('127.0.0.1', 27017)
    # table = conn.TaoBaoScrapyDB.ALLStoreTB
    try:
        if conn.TaoBaoScrapyDB.ALLStoreTB.insert(result):
            print '保存数据成功'
    except Exception as e:
        print('保存到MONGODB失败', e)

#在每次任务开始时，先创建一张临时表，保存所有宝贝ID
def crate_temTable():
    dbconn = mongodbConn()
    dbconn.connect()
    conn = dbconn.getConn()

    # result = conn.TaoBaoScrapyDB.TaoBaoSTB.find({'yearAndMonth':time.strftime('%Y-%m-%d',time.localtime(time.time()))})
    # result = conn.TaoBaoScrapyDB.TaoBaoSTB.find({"$or": [{"state": "进行中"}, {"state": "待开启"}]})

    # result = conn.TaoBaoScrapyDB.TaoBaoSTB.find({"$or": [{"state": "进行中"}, {"state": "待开启"}]})
    try:
        conn.TaoBaoScrapyDB.TaoBaoSTB.aggregate(
            [
                {'$project': {
                    'ID': '$ID',
                    'customized':'$customized',
                    'itemID':'$itemID',
                    'yearAndMonth':'$yearAndMonth'
                }},
                {'$match': {'yearAndMonth': {'$eq': time.strftime('%Y-%m-%d',time.localtime(time.time()))}}},
                {'$out': 'allStoreResults'}
            ]
        )

    except Exception as e:
        print '3Error in parse ---%s'%e
    finally:
        # dbconn.close()
        # conn = None
        print 'finish******2'
        dele_repeat()

#先进行去重操作
def dele_repeat():
    dbconn = mongodbConn()
    dbconn.connect()
    conn = dbconn.getConn()
    table = conn.TaoBaoScrapyDB.allStoreResults  # 获取数据库中的所有关键字条件
    try:
        for url in table.distinct('ID'):  # 使用distinct方法，获取每一个独特的元素列表
            num = table.count({"ID": url})  # 统计每一个元素的数量
            print num
            for i in range(1, num):  # 根据每一个元素的数量进行删除操作，当前元素只有一个就不再删除
                print 'delete %s %d times ' % (url, i)
                # 注意后面的参数， 很奇怪，在mongo命令行下，它为1时，是删除一个元素，这里却是为0时删除一个
                table.remove({"ID": url}, 0)
            # for i in table.find({"ID": url}):  # 打印当前所有元素
            #     print i
    except Exception as e:
        pass
    finally:
        # dbconn.close()
        # dbconn = None
        print 'finish******3'


# if __name__ == '__main__':
#     allStoreScrapy()
#     # crate_temTable()
#
#     # conneStore()


















