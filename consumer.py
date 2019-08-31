import requests
from queue import Queue
import random
from lxml import etree
import hashlib
import re
import json
from multiprocessing.pool import ThreadPool
import time

from x_sql import my_sql
from setting import USER_AGENT,SPEEN
"""消费url_info表中的所有url"""
#线程池   队列
q = Queue()
sql = my_sql()


class Consumer_spider(object):
    #自己去重
    completed = set()
    def add_url(self):
        url = sql.find_info("select url form url_info")
        urls = [a[0] for a in url]
        for url in urls:
            print(url)
            q.put(url)

    def requests(self, url):
        res = requests.get(url, headers=random.choice(USER_AGENT))
        return res

    def parse_detail(self, res):
        """解析页面  返回解析后的数据字典"""
        url = res.url
        print(f"获得{url}页面")
        html = etree.HTML(res.text)
        article = ';'.join(html.xpath('//div[@class="rich_media_content "]//p//span//text()'))
        # 利用生成指纹+缓存去重
        m = hashlib.md5(article.encode("utf-8"))
        fingerprint = m.hexdigest()
        if fingerprint in self.completed:
            return
        title = re.sub(r"\s", "", html.xpath('//h2[@class="rich_media_title"]/text()')[0])
        author = re.sub(r"\s", "", re.findall('var title ="(.*?)";', res.text)[0])
        images = re.findall(r'<img.*?data-ratio=".*?".*?data-src="(.*?)"', res.text)
        images = [img.rstrip() for img in images]
        update_time = re.findall(r'var t="\d+",n="\d+",s="(.*?)";', res.text)[0]
        data = {
            "url": url,
            "title": title,
            "article": article,
            "images": images,
            "update_time": update_time,
            "author": author,
            "id": fingerprint
        }
        self.completed.add(fingerprint)
        return data

    def do_storage(self, data):
        """存数据"""
        sql_language = "SELECT id FROM id_info"
        info_id = sql.find_info(sql_language)
        ids = [a[0] for a in info_id]
        if data["id"] in ids:
            print("已存在：{}".format(data["id"]))
            return
        sql_language = "insert into json_info(id,url,title,article,images,update_time,author) value('%s', '%s', '%s', '%s', '%s', '%s', '%s')"%\
                       (data["id"], data["url"], data["title"], data["article"], json.dumps(data["images"]), data["update_time"], data["author"])
        sql.inser(sql_language)
        sql_language = "insert into id_info(id) value('%')"%(data['id'])
        sql.inser(sql_language)
        print("存储完毕")
        data = json.dumps(data)
        print(data)
        return json.loads(data)

    def run(self, url):
        res = self.requests(url)
        data = self.parse_detail(res)
        self.do_storage(data)


def do_consumer():
    pool = ThreadPool(SPEEN)
    spider = Consumer_spider()
    spider.all_url()
    while not q.empty():
        for i in range(SPEEN):
            pool.apply_async(spider.run,args=(q.get(),))
        pool.close()
        pool.join()
        time.sleep(random.randint(1,5))