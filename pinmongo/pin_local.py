from pymongo import MongoClient

class Pin_local():
    def __init__(self, username=None, password=None, host='localhost', port=27017):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.db_name = 'pin'
        if username and password:
            client = MongoClient(host=host,port=port,username=username,password=password)
        else:
            client = MongoClient(host,port)
        self.db = client[self.db_name]
    @property
    def pics(self):
        return self.db['pics']
