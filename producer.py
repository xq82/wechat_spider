import requests
import setting
import random
import time

from x_sql import my_sql
sql = my_sql()
"""将生产出来的所有url存入sql中"""

def get_cook_and_token():
    return random.choice(setting.COOKIES)


class Producer(object):
    def __init__(self,start=0,end=1, fakeid=None):
        self.start = start
        self.end = end
        self.fakeid = fakeid

    def task_url(self):
        """生成列表页爬取的url以及headers"""
        for page in range(self.start, self.end):
            token_and_cookies = get_cook_and_token()
            data = {
                "token": token_and_cookies["token"],
                "f": "json",
                "random": random.random(),
                "action": "list_ex",
                "begin": page * 5,
                "count": "5",
                "fakeid": self.fakeid,
                "type": 9,
                "lang": "zh_CN",
                "ajax": 1
            }
            headers = {
                'Cookie': token_and_cookies["cookies"],
                "Host": "mp.weixin.qq.com",
                "User-Agent": random.choice(setting.USER_AGENT)
            }
            yield [data, headers]

    def request(self, data=None, headers=None):
            if data:
                if not headers:
                    return
                res = requests.get("https://mp.weixin.qq.com/cgi-bin/appmsg", params=data, headers=headers)
                return res

    def detail_url(self, res):
        print(res.text)
        try:
            new_urls = res.json()["app_msg_list"]
            for new_url in new_urls:
                url = new_url["link"]
                self.add_url(url)
        except Exception as e:
            print("登陆超时或者访问受限：", e)
            with open("url.txt", "a+", encoding="utf-8") as f:
                f.write(res.url+"\n")

    def add_url(self, url):
        """将获取到的url加入sql的url表中"""
        sql.inser("insert into url_info(url) VALUE('%s')"%url)

    def run(self):
        i = 0
        for data_headers in self.task_url():
            res = self.request(data_headers[0], data_headers[1])
            self.detail_url(res)
            i += 1
            print("第%s页数据"%i)
            print("暂停三秒")
            time.sleep(3)