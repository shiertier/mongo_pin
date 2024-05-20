from .mongo_cloud import _Mongo_cloud
import os


class Pin_cloud(_Mongo_cloud):
    def __init__(self,username,password):
        self.db_name = 'pin'
        self._script_name = __file__
        self._script_path = os.path.abspath(__file__)
        self._script_directory = os.path.dirname(self._script_path)
        self._account_file = os.path.join(self._script_directory, 'mongo_dovaban497_2')
        self.uri = self.account_file_load(self._account_file)
        self.mongo_username = username
        self.mongo_password = password
        self.db = {}
        self.clients = {}
        self._get_clients()
        self._login()
