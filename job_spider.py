#!/usr/bin/env python
# coding=utf-8
import time
from gevent import monkey
from gevent.pool import Pool
from queue import Queue
import os
from pprint import pprint
import csv
from collections import Counter

from bs4 import BeautifulSoup
import requests
import matplotlib.pyplot as plt
import jieba
from wordcloud import WordCloud
import pymysql

monkey.patch_all()


class JobSpider:
    """
    51 job 网站爬虫类
    """

    def __init__(self):
        # 记录当前爬第几条数据
        self.count = 1
        self.company = []
        self.text = ""
        self.headers = {
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36'
                          '(KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
        }
        # 线程池队列
        self.desc_url_queue = Queue()
        # 线程池管理线程,最大协程数
        self.pool = Pool(8)

    def job_spider(self):
        """
        爬虫入口
        """
        url = "http://search.51job.com/list/010000%252C020000%252C030200%252C" \
              "040000,000000,0000,00,9,99,Python,2,{}.html? lang=c&stype=1&" \
              "postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99" \
              "&companysize=99&lonlat=0%2C0&radius=-1&ord_field=0&confirmdate=9" \
              "&fromType=1&dibiaoid=0&address=&line=&specialarea=00&from=&welfare="
        urls = [url.format(p) for p in range(1, 14)]
        for url in urls:
            print('爬取第{}页'.format(urls.index(url) + 1))
            r = requests.get(url, headers=self.headers).content.decode('gbk')
            bs = BeautifulSoup(r, 'lxml').find(
                "div", class_="dw_table").find_all("div", class_="el")
            for b in bs:
                try:
                    href, post = b.find('a')['href'], b.find('a')['title']
                    locate = b.find('span', class_='t3').text
                    salary = b.find('span', class_='t4').text
                    d = {
                        'href': href,
                        'post': post,
                        'locate': locate,
                        'salary': salary
                    }
                    # 岗位详情链接加入队列
                    self.desc_url_queue.put(href)
                    self.company.append(d)
                except Exception:
                    pass
        # 打印队列长度,即多少条岗位详情url
        print('队列长度为{}'.format(self.desc_url_queue.qsize()))

    def post_require(self):
        """
        爬取职位描述
        """
        # for c in self.company:
        while True:
            # 从队列中取url
            url = self.desc_url_queue.get()

            r = requests.get(
                url, headers=self.headers)
            if r.status_code == 200:
                print('----爬取第{}条岗位详情----'.format(self.count))
                r = r.content.decode('gbk')
                self.desc_url_queue.task_done()
                self.count += 1
            else:
                self.desc_url_queue.put(url)
                continue
            try:
                bs = BeautifulSoup(r, 'lxml').find(
                    'div', class_="bmsg job_msg inbox").text
                s = bs.replace("微信", "").replace("分享", "").replace("邮件", "").replace("\t", "").strip()
                # self.text += s
                # print(self.text)
                with open(os.path.join("data", "post_require_new.txt"),
                          "a", encoding="utf-8") as f:
                    f.write(s)
            except Exception as e:
                print(e)
                print('第{}条岗位详情解析出错'.format(self.count))
                print(url)

    @staticmethod
    def post_desc_counter():
        """
        职位描述统计
        """
        # import thulac
        post = open(os.path.join("data", "post_require.txt"),
                    "r", encoding="utf-8").read()
        # 使用 thulac 分词
        # thu = thulac.thulac(seg_only=True)
        # thu.cut(post, text=True)

        # 使用 jieba 分词
        file_path = os.path.join("data", "user_dict.txt")
        jieba.load_userdict(file_path)
        seg_list = jieba.cut(post, cut_all=False)
        counter = dict()
        for seg in seg_list:
            counter[seg] = counter.get(seg, 1) + 1
        counter_sort = sorted(
            counter.items(), key=lambda value: value[1], reverse=True)
        pprint(counter_sort)
        with open(os.path.join("data", "post_pre_desc_counter.csv"),
                  "w+", encoding="utf-8") as f:
            f_csv = csv.writer(f)
            f_csv.writerows(counter_sort)

    def post_counter(self):
        """
        职位统计
        """
        lst = [c.get('post') for c in self.company]
        counter = Counter(lst)
        counter_most = counter.most_common()
        pprint(counter_most)
        with open(os.path.join("data", "post_pre_counter.csv"),
                  "w+", encoding="utf-8") as f:
            f_csv = csv.writer(f)
            f_csv.writerows(counter_most)

    def post_salary_locate(self):
        """
        招聘大概信息，职位，薪酬以及工作地点
        """
        lst = []
        for c in self.company:
            lst.append((c.get('salary'), c.get('post'), c.get('locate')))
        pprint(lst)
        file_path = os.path.join("data", "post_salary_locate.csv")
        with open(file_path, "w+", encoding="utf-8") as f:
            f_csv = csv.writer(f)
            f_csv.writerows(lst)

    @staticmethod
    def post_salary():
        """
        薪酬统一处理
        """
        mouth = []
        year = []
        thousand = []
        with open(os.path.join("data", "post_salary_locate.csv"),
                  "r", encoding="utf-8") as f:
            f_csv = csv.reader(f)
            for row in f_csv:
                if "万/月" in row[0]:
                    mouth.append((row[0][:-3], row[2], row[1]))
                elif "万/年" in row[0]:
                    year.append((row[0][:-3], row[2], row[1]))
                elif "千/月" in row[0]:
                    thousand.append((row[0][:-3], row[2], row[1]))
        # pprint(mouth)

        calc = []
        for m in mouth:
            s = m[0].split("-")
            calc.append(
                (round(
                    (float(s[1]) - float(s[0])) * 0.4 + float(s[0]), 1),
                 m[1], m[2]))
        for y in year:
            s = y[0].split("-")
            calc.append(
                (round(
                    ((float(s[1]) - float(s[0])) * 0.4 + float(s[0])) / 12, 1),
                 y[1], y[2]))
        for t in thousand:
            s = t[0].split("-")
            calc.append(
                (round(
                    ((float(s[1]) - float(s[0])) * 0.4 + float(s[0])) / 10, 1),
                 t[1], t[2]))
        pprint(calc)
        with open(os.path.join("data", "post_salary.csv"),
                  "w+", encoding="utf-8") as f:
            f_csv = csv.writer(f)
            f_csv.writerows(calc)

    @staticmethod
    def post_salary_counter():
        """
        薪酬统计
        """
        with open(os.path.join("data", "post_salary.csv"),
                  "r", encoding="utf-8") as f:
            f_csv = csv.reader(f)
            lst = [row[0] for row in f_csv]
        counter = Counter(lst).most_common()
        pprint(counter)
        with open(os.path.join("data", "post_salary_counter1.csv"),
                  "w+", encoding="utf-8") as f:
            f_csv = csv.writer(f)
            f_csv.writerows(counter)

    @staticmethod
    def world_cloud():
        """
        生成词云
        """
        counter = {}
        with open(os.path.join("data", "post_pre_desc_counter.csv"),
                  "r", encoding="utf-8") as f:
            f_csv = csv.reader(f)
            for row in f_csv:
                counter[row[0]] = counter.get(row[0], int(row[1]))
            pprint(counter)
        file_path = os.path.join("font", "msyh.ttf")
        wc = WordCloud(font_path=file_path,
                       max_words=100,
                       height=600,
                       width=1200).generate_from_frequencies(counter)
        plt.imshow(wc)
        plt.axis('off')
        plt.show()
        wc.to_file(os.path.join("images", "wc.jpg"))

    @staticmethod
    def insert_into_db():
        """
        插入数据到数据库

        create table jobpost(
            j_salary float(3, 1),
            j_locate text,
            j_post text
        );
        """
        conn = pymysql.connect(host="localhost",
                               port=3306,
                               user="root",
                               passwd="0303",
                               db="chenx",
                               charset="utf8")
        cur = conn.cursor()
        with open(os.path.join("data", "post_salary.csv"),
                  "r", encoding="utf-8") as f:
            f_csv = csv.reader(f)
            sql = "insert into jobpost(j_salary, j_locate, j_post) values(%s, %s, %s)"
            for row in f_csv:
                value = (row[0], row[1], row[2])
                try:
                    cur.execute(sql, value)
                    conn.commit()
                except Exception as e:
                    print(e)
        cur.close()

    # 协程池接收请求任务,可以扩展把解析,存储耗时操作加入各自队列,效率最大化
    def excute_more_task(self, target, count):
        for i in (0, count):
            self.pool.apply_async(target)

    # 多线程爬取数据
    def run(self):
        self.job_spider()
        self.excute_more_task(self.post_require, 5)
        time.sleep(0.01)
        # 主线程阻塞,等待队列清空
        self.desc_url_queue.join()


if __name__ == "__main__":
    spider = JobSpider()
    start = time.time()
    # 开始爬取
    spider.run()
    end = time.time()
    print('总耗时{}'.format(end - start))
    # spider.job_spider()
    # 按需启动
    # spider.post_salary_locate()
    # spider.post_salary()
    # spider.insert_into_db()
    # spider.post_salary_counter()
    # spider.post_counter()
    # spider.world_cloud()
