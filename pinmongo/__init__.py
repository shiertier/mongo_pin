from .pin_scrapy import Pin_scrapy
from .pin_local import Pin_local
from .pin_cloud import Pin_cloud
from pymongo.errors import BulkWriteError

def insert_many(collection,data):
    try:
        collection.insert_many(data,ordered = False)
    except BulkWriteError as e:
        #print("Error occurred:", e.details)
        print("插入:", e.details['nInserted'])
        failure_num = len(data) - int(e.details['nInserted'])
        print('失败:', failure_num)