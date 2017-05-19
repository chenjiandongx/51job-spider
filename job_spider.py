from pprint import pprint
import csv
import requests
from bs4 import BeautifulSoup
import jieba

class JobSpider():

    def __init__(self):
        self.company = []
        self.text = ""
        self.headers = {'X-Requested-With': 'XMLHttpRequest',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                      'Chrome/56.0.2924.87 Safari/537.36'}

    def job_spider(self):
        """ 爬虫入口 """
        url = "http://search.51job.com/list/010000%252C020000%252C030200%252C040000,000000,0000,00,9,99,Python,2,{}.html?" \
              "lang=c&stype=1&postchannel=0000&workyear=99&cotype=99&degreefrom=99&jobterm=99&companysize=99&lonlat=0%2C0" \
              "&radius=-1&ord_field=0&confirmdate=9&fromType=1&dibiaoid=0&address=&line=&specialarea=00&from=&welfare="
        urls = [url.format(p) for p in range(1, 14)]
        for url in urls:
            r = requests.get(url, headers=self.headers).content.decode('gbk')
            bs = BeautifulSoup(r, 'lxml').find("div", class_="dw_table").find_all("div", class_="el")
            for b in bs:
                try:
                    href, post = b.find('a')['href'], b.find('a')['title']
                    locate = b.find('span', class_='t3').text
                    salary = b.find('span', class_='t4').text
                    d = {'href':href, 'post':post, 'locate':locate, 'salary':salary}
                    self.company.append(d)
                except Exception:
                    pass

    def post_require(self):
        """ """
        for c in self.company:
            r = requests.get(c.get('href'), headers=self.headers).content.decode('gbk')
            bs = BeautifulSoup(r, 'lxml').find('div', class_="bmsg job_msg inbox").text
            s = bs.replace("举报", "").replace("分享", "").replace("\t", "").strip()
            self.text += s
        # print(self.text)
        with open(r".\data\post_require.txt", "w+", encoding="utf-8") as f:
            f.write(self.text)

    def post_counter(self):
        """ """
        # import thulac
        post = open(r".\data\post_require.txt", "r", encoding="utf-8").read()
        # 使用 thulac 分词
        # thu = thulac.thulac(seg_only=True)
        # thu.cut(post, text=True)

        # 使用 jieba 分词
        jieba.load_userdict(r".\data\user_dict.txt")
        seg_list = jieba.cut(post, cut_all=False)
        counter = dict()
        for seg in seg_list:
            counter[seg] = counter.get(seg, 1) + 1
        counter_sort = sorted(counter.items(), key=lambda value: value[1], reverse=True)
        pprint(counter_sort)
        with open(r".\data\pre_counter.csv", "w+", encoding="utf-8") as f:
            f_csv = csv.writer(f)
            f_csv.writerows(counter_sort)

    def post_salary_locate(self):
        """ """
        lst = []
        for c in self.company:
            lst.append((c.get('salary'), c.get('post'), c.get('locate')))
        pprint(lst)
        with open(r".\data\post_salary_locate.csv", "w+", encoding="utf-8") as f:
            f_csv = csv.writer(f)
            f_csv.writerows(lst)

if __name__ == "__main__":
    spider = JobSpider()
