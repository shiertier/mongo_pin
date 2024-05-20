from pymongo import MongoClient
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

class _Mongo_cloud():
    def __init__(self, db_name, uri, mongo_username,mongo_password):
        self.db_name = db_name
        self.uri = uri
        self.db = {}
        self.clients = {}
        self.mongo_username = mongo_username
        self.mongo_password = mongo_password
        self._get_clients()
        self._login()

    def account_file_load(self,file_path):
        dic = {}
        with open(file_path, 'r') as f:
            # 每行是一条数据，格式为key,value
            for line in f.readlines():
                # 去除换行符
                line = line.strip()
                # 分割key和value
                key, value = line.split(',')
                # 将key和value存入字典
                dic[key] = value
        return dic

    def _get_clients(self):
        for key,uri in self.uri.items():
            # ac-o8xtffk-shard-00-01.pamdkud.mongodb.net
            uri1 = uri.split('.')[0]
            uri2 = uri.split('.')[1]
            self.clients[key] = MongoClient(f"mongodb://{self.mongo_username}:{self.mongo_password}@ac-{uri1}-shard-00-01.{uri2}.mongodb.net:27017/?tls=true")
    def _login(self):
        for key,client in self.clients.items():
            exec(f'self.pics{key} = client[self.db_name]["pics"]')

    def _get_unique(self):
        indexes = self.pics1.list_indexes()
        self.unique_indexes = [index for index in indexes if index.get('unique')] + ["_id"]

    def _find_one(self, collection, query, sort_query=None):
        return collection.find_one(query)

    def _find(self, collection, query, sort_query=None, limit=None):
        if sort_query is not None and limit is not None:
            return list(collection.find(query).sort(sort_query).limit(limit))
        elif sort_query is not None:
            return list(collection.find(query).sort(sort_query))
        elif limit is not None:
            return list(collection.find(query).limit(limit))
        else:
            return list(collection.find(query))

    def _delete(self, collection, query):
        return collection.delete_many(query)

    def find_one(self, query={}):
        results = []
        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = [executor.submit(self._find_one, eval(f'self.pics{key}'), query) for key in self.uri.keys()]
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as exc:
                    print(f'    error | {exc}')
        if len(results) == 0:
            result = None
        else:
            result = results[0]
        return result

    def find_one_random(self, query={}):
        thekey = list(self.uri.keys())
        random.shuffle(thekey)
        for key in thekey:
            count = eval(f'self.pics{key}.count_documents(query)')
            if count:
                pipeline = [
                    {'$match': query},
                    {'$sample': {'size': 1}}
                ]
                result = eval(f'self.pics{key}.aggregate(pipeline).next()')
                if result:
                    return result
        return None


    def find(self, query={}, sort_query=None, limit=None):
        print(f'searching {query} ...')
        results = []
        with ThreadPoolExecutor(max_workers=100) as executor:
            if sort_query is not None and limit is not None:
                futures = [executor.submit(self._find, eval(f'self.pics{key}'), query, sort_query, limit) for key in self.uri.keys()]
            elif sort_query is not None:
                futures = [executor.submit(self._find, eval(f'self.pics{key}'), query, sort_query) for key in self.uri.keys()]
            elif limit is not None:
                futures = [executor.submit(self._find, eval(f'self.pics{key}'), query, limit=limit) for key in self.uri.keys()]
            else:
                futures = [executor.submit(self._find, eval(f'self.pics{key}'), query) for key in self.uri.keys()]
        for future in as_completed(futures):
            try:
                result = future.result()
                for r in result:
                    results.append(r)  # 将结果添加到列表中
            except Exception as exc:
                print(f'    error | {exc}')
        if len(results) == 0:
            results = None
        if sort_query is not None:
            sort1, sort2 = list(sort_query.items())[0]  # sort1决定排序的字段，sort2决定排序的方式（1为正序，-1为倒序）
            results.sort(key=lambda x: x[sort1], reverse=False if sort2 == 1 else True)
        if limit is not None:
            results = results[:limit]
        return results  # 返回所有查询结果的列表

    def find_last_one(self):
        return self.find({},sort_query={'_id':-1},limit=1)

    def delete(self, query):
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(self._delete, eval(f'self.pics{key}'), query) for key in self.uri.keys()]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f'    error | {exc}')

    def test_link(self):
        for key in self.uri.keys():
            print(key)
            result = eval(f'self.pics{key}.find_one()')

    def _insert_base(self, data):
        randon_key = random.choice(list(self.uri.keys()))
        exec(f'self.pics{randon_key}.insert_one(data)')

    def _insert_one(self, data, ignore = False):
        # 对于所有独一性索引进行查询，需要data包含所有字段
        if not ignore:
            query_list = []
            for e in self.unique_indexes:
                if e not in data:
                    raise ValueError(f'字典中必须包含所有独一性索引字段，缺失字段: {e}')
                query_list.append({e: data[e]})
            exists_data = self.find_one({'$or': query_list})
        else:
            exists_data = None
        if exists_data:
            print(f"    insert | repeat | {[data[e] for e in self.unique_indexes]}")
        else:
            self._insert_base(data)

    def insert(self,datas,ignore = False):
        # 检查data是字典还是列表
        if isinstance(datas, dict):
            self._insert_one(datas)
        elif isinstance(datas, list):
            with ThreadPoolExecutor(max_workers=500) as executor:
                futures = [executor.submit(self._insert_one, data,ignore) for data in datas]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        print(f'    error | {exc}')
        else:
            raise ValueError('输入数据必须为字典或列表')

    def _update(self,collection,query,update):
        return collection.update_many(query, update)

    def update_or_create_one(self, query, update):
        # query 必须包含_id字段
        if '_id' not in query:
            raise ValueError('query条件必须包含_id字段')
        if self.find_one(query):
            self.update(query, update)
        else:
            self.insert_one(query)

    def update(self, query, update):
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(self._update, eval(f'self.pics{key}'), query, update) for key in self.uri.keys()]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f'    error | {exc}')

    def create_index(self, index):
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(eval(f'self.pics{key}').create_index(index), index) for key in self.uri.keys()]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f'    error | {exc}')

    def drop_index(self, index):
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(eval(f'self.pics{key}').drop_index(index), index) for key in self.uri.keys()]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f'    error | {exc}')

    def _count(self, collection, query):
        return collection.count_documents(query)

    def count_doc(self,query={}):
        results = []
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(self._count, eval(f'self.pics{key}'), query) for key in self.uri.keys()]
            count = 0
            for future in as_completed(futures):
                try:
                    result = future.result()
                    count += result
                except Exception as exc:
                    print(f'    error | {exc}')
        return count  # 返回所有查询结果的列表