import requests
import json
from bs4 import BeautifulSoup
from requests.exceptions import SSLError
import time

class Pin_scrapy():
    def __init__(self):
        self.HEADERS = {'user-agent': 'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)'}

    def _get(self, url, error_count=0):
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            return response
        except SSLError as e:
            print('SSLError')
        except Exception as e:
            print(e)
        error_count += 1
        time.sleep(2)
        if error_count < 5:
            return self._get(url, error_count)
        else:
            raise e


    def _pic_relate_pics_1page(self, pic_id, bookmark='', page_size=50):
        url = f'https://www.pinterest.com/resource/RelatedModulesResource/get/?data=%7B%22options%22%3A%7B%22pin_id%22%3A%22{pic_id}%22%2C%22context_pin_ids%22%3A%5B%5D%2C%22page_size%22%3A{page_size}%2C%22search_query%22%3A%22%22%2C%22source%22%3A%22deep_linking%22%2C%22top_level_source%22%3A%22deep_linking%22%2C%22top_level_source_depth%22%3A1%2C%22is_pdp%22%3Afalse%2C%22bookmarks%22%3A%5B{bookmark}%5D%7D%2C%22context%22%3A%7B%7D%7D&_=1713899242723'
        return self._get(url)

    def _board_relate_pics_1page(self, board_id, bookmark='', page_size=50):
        url = f"https://www.pinterest.com/resource/BoardContentRecommendationResource/get/?data=%7B%22options%22%3A%7B%22add_vase%22%3Atrue%2C%22id%22%3A%22{board_id}%22%2C%22type%22%3A%22board%22%2C%22__track__referrer%22%3A20%2C%22page_size%22%3A{page_size}%2C%22bookmarks%22%3A%5B{bookmark}%5D%7D%2C%22context%22%3A%7B%7D%7D&_=1713895163061"
        return self._get(url)

    def _board_pics_1page(self, board_id,bookmark='', page_size=50):
        url = f"https://www.pinterest.com/resource/BoardFeedResource/get/?data=%7B%22options%22%3A%7B%22add_vase%22%3Atrue%2C%22board_id%22%3A%22{board_id}%22%2C%22field_set_key%22%3A%22react_grid_pin%22%2C%22filter_section_pins%22%3Afalse%2C%22is_react%22%3Atrue%2C%22prepend%22%3Afalse%2C%22page_size%22%3A{page_size}%2C%22bookmarks%22%3A%5B{bookmark}%5D%7D%2C%22context%22%3A%7B%7D%7D&_=1713873963342"
        return self._get(url)

    def _board_r_to_dict(self, board_id, bookmark='',relate = False):
        if relate:
            response = self._board_relate_pics_1page(board_id,bookmark)
        else:
            response = self._board_pics_1page(board_id, bookmark)
        data = response.json()
        bookmark = data['resource']['options']['bookmarks'][0]
        bookmark = '%22' + bookmark + '%22'
        pics_data = data['resource_response']['data']
        pics = []
        for p in pics_data:
            try:
                saves = p['aggregated_pin_data']['aggregated_stats']['saves'][0],
            except Exception:
                saves = None
            try:
                creator = p['native_creator']['username'][0],
                creator_id = p['native_creator']['id']
            except TypeError:
                creator = None
                creator_id = None
            pic = {
                '_id': p['id'],
                'link': p['link'],
                'saves': saves,
                'pic': p['images']['orig']['url'],
                'width': p['images']['orig']['width'],
                'height': p['images']['orig']['height'],
                'visual_annotation': p['pin_join']['visual_annotation'],
                'creator': creator,
                'creator_id': creator_id,
                'dominant_color': p['dominant_color'],
                }
            pics.append(pic)
        return pics, bookmark


    def _load_pin(self,pin_id):
        LOAD_PIN_URL_FORMAT = "https://www.pinterest.com/pin/{}/"
        resp = self._get(url=LOAD_PIN_URL_FORMAT.format(pin_id))
        soup = BeautifulSoup(resp.text, "html.parser")
        scripts = soup.findAll("script")
        pin_data = None
        for s in scripts:
            if "data-relay-response" in s.attrs and s.attrs["data-relay-response"] == "true":
                pinJsonData = json.loads(s.contents[0])["response"]["data"]["v3GetPinQuery"]["data"]
                pin_data = pinJsonData
        if pin_data:
            return pin_data

        raise Exception(f"{pin_id} Pin data not found.")

    def _pics_r_to_dict(self,pic_id,bookmark=''):
        response = self._pic_relate_pics_1page(pic_id, bookmark)
        data = response.json()
        bookmark = data['resource']['options']['bookmarks'][0]
        bookmark = '%22' + bookmark + '%22'
        pics_data = data['resource_response']['data']
        pics = []
        num = 1
        for p in pics_data:
            if num ==1:
                num += 1
                continue
            try:
                saves = p['aggregated_pin_data']['aggregated_stats']['saves'][0],
            except Exception:
                saves = None
            try:
                creator = p['native_creator']['username'][0],
                creator_id = p['native_creator']['id']
            except Exception:
                creator = None
                creator_id = None
            pic = {
                '_id': p['id'],
                'link': p['link'],
                'saves': saves,
                'pic': p['images']['orig']['url'],
                'width': p['images']['orig']['width'],
                'height': p['images']['orig']['height'],
                'creator': creator,
                'creator_id': creator_id,
                'dominant_color': p['dominant_color'],
                }
            pics.append(pic)
        return pics, bookmark

    def username_boards(self, username):
        url = f'https://www.pinterest.com/resource/BoardsResource/get/?data=%7B%22options%22%3A%7B%22privacy_filter%22%3A%22all%22%2C%22sort%22%3A%22last_pinned_to%22%2C%22field_set_key%22%3A%22profile_grid_item%22%2C%22filter_stories%22%3Afalse%2C%22username%22%3A%22{username}%22%2C%22page_size%22%3A25%2C%22group_by%22%3A%22mix_public_private%22%2C%22include_archived%22%3Atrue%2C%22redux_normalize_feed%22%3Atrue%7D%2C%22context%22%3A%7B%7D%7D&_=1713898041896'
        response = self._get(url)
        datas = response.json()['resource_response']['data']
        ids = []
        num = 1
        for data in datas :
            id = data['id']
            if num > 1:
                ids.append(id)
            num += 1
        return ids

    def board_pics(self,board_id,relate=False):
        pics_all = []
        bookmark = ''
        while not bookmark == '%22-end-%22':
            pics, bookmark = self._board_r_to_dict(board_id, bookmark, relate)
            pics_all.extend(pics)
            print(len(pics_all))
        return pics_all

    def pics_relate_pics(self,pic_id):
        pics_all = []
        bookmark = ''
        while not bookmark == '%22-end-%22':
            pics, bookmark = self._pics_r_to_dict(pic_id, bookmark)
            pics_all.extend(pics)
            print(len(pics_all))
        return pics_all

    def get_pic_add_data(self,pic_id):
        data = self._load_pin(pic_id)
        va = data['pinJoin']['visualAnnotation']
        repinCount = data['repinCount']
        va,repinCount
        return va,repinCount