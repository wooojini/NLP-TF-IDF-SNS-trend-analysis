##크롤러 슬랙버전

import os
import pandas
import csv
import time
import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import selenium.common.exceptions 
from bs4 import BeautifulSoup
from threading import Lock
import logging

class Crawler:
    def __init__(self):
        self.keyword = None
        
    def set_keyword(self,keyword):
        self.keyword = keyword
        
    def get_keyword(self):
        return self.keyword
    
    ##링크 추출 함수
    def get_links(self):
        pass
    
    ##게시물 추출 함수
    def get_contents(self,link):
        pass
    
class InstagramCrawler(Crawler):
    def __init__(self):
        self.lock = Lock()
        
        Crawler.__init__(self)
        self.link_crawler = None
        self.url = 'https://www.instagram.com'
        self.chrome_path = 'C:\chromedriver_win32\chromedriver.exe'
        
        ##크롬 옵션 설정
        self.chromeOptions = Options()
        self.chromeOptions.add_argument('--ignore-certificate-errors')  #SSL 관련 오류 무시
        self.chromeOptions.add_argument('--ignore-ssl-errors')  #SSL 관련 오류 무시
        
        ##data디렉토리 및 파일 생성
        self.create_data_storage()
        
        ##로거 생성
        self.logger = logging.getLogger('test_log')
        self.logger.setLevel(logging.DEBUG)
        
        ##로그포맷팅
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        ##파일 핸들러
        fileHandler = logging.FileHandler(self.filepath_logging)
        fileHandler.setLevel(logging.DEBUG)
        fileHandler.setFormatter(formatter)
        
        ##스트림 핸들러
        streamHandler = logging.StreamHandler()
        streamHandler.setLevel(logging.DEBUG)
        streamHandler.setFormatter(formatter)
        
        ##핸들러 등록
        self.logger.addHandler(fileHandler)
        self.logger.addHandler(streamHandler)
            
    ##파일저장소 생성 함수
    def create_data_storage(self):
        self.filepath_storage = './rawdata'
        if os.access(self.filepath_storage,os.F_OK) == False:
            os.mkdir(self.filepath_storage)
            
        self.filepath_links = './rawdata/links_instagram.csv'
        if os.access(self.filepath_links,os.F_OK) == False:
            new_data = {'keyword':[],'link':[],'crawling':[]}
            df = pandas.DataFrame(new_data,columns=['keyword','link','crawling'])
            df.to_csv(self.filepath_links,index=False, encoding='utf-8')
              
        self.filepath_contents = './rawdata/contents_instagram.csv'
        if os.access(self.filepath_contents,os.F_OK) == False:
            new_data = {'keyword':[],'text':[],'tags':[],'date':[]}
            df = pandas.DataFrame(new_data,columns=['keyword','text','tags','date'])
            df.to_csv(self.filepath_contents,index=False, encoding='utf-8')
        
        ##로깅파일 폴더
        self.filepath_logging ='./logging/logfile.log'
        if os.access('./logging',os.F_OK) == False:
            os.mkdir('./logging')
            
    ##키워드 검색 함수
    def search_keyword(self):
        ##예외처리
        if self.keyword == None : 
            return False
        if self.link_crawler == None : ##크롬드라이버 실행
            self.link_crawler = webdriver.Chrome(self.chrome_path,chrome_options=self.chromeOptions)
            
        ##링크 크롤러 실행
        url = self.url+'/explore/tags/'+self.keyword
        if self.connect_page(url) != None:
            self.link_crawler.get(url)
            self.link_crawler.implicitly_wait(3)
            self.close_dialog_box() ##팝업 제거
        else :
            self.logger.debug("error - search keyword - None instance : link connect error")
            return False ##링크 접속 에러
        return True
    
    ##팝업창 제거 함수
    def close_dialog_box(self):
        try:
            self.link_crawler.find_element_by_css_selector('#react-root > section > nav > div._8MQSO.Cx7Bp > \
                                                           div > div > div.ctQZg > div > div > div > button').click()
        except selenium.common.exceptions.NoSuchElementException : 
            pass
    
    @staticmethod
    def connecton(url):
        res = None
        delay_time = 1
        
        ##서버응답오류 처리
        try:
            res = requests.get(url)
        except requests.exceptions.ConnectTimeout :
            time.sleep(delay_time)
            res = requests.get(url)
        except requests.exceptions.ReadTimeout :
            time.sleep(delay_time)
            res = requests.get(url)    
        except requests.exceptions.Timeout :
            time.sleep(delay_time)
            res = requests.get(url)
        except :
            res = None
        
        return res
        
    ##페이지 접속 가능여부
    def connect_page(self,url,link=None):
        res = InstagramCrawler.connecton(url)

        if res == None :
            return res
        
        ##응답오류 처리 (페이지 접속)
        error_status_code = [400,404,405] 
        if res.status_code in error_status_code:
            self.logger.debug("error - connect page - status code : %d" % res.status_code)
            if link != None : 
                self.delete_link(link) ##링크파일 링크삭제
            return None
            
        return res
        
    ##링크 추출 함수
    def get_links(self):
        SCROLL_PAUSE_TIME = 0.5
        CRAWLING_COUNT = 200
        delay_time = 1
        
        ##키워드 검색
        if self.search_keyword() == False :
            self.link_crawler.quit() ##크롬드라이버 종료
        
        cnt = 0
        ##크롬창 스크롤 내리기
        while True:
            try:
                last_height = self.link_crawler.execute_script("return document.body.scrollHeight")
                while True:
                    self.link_crawler.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(SCROLL_PAUSE_TIME)
                    new_height = self.link_crawler.execute_script("return document.body.scrollHeight")

                    if new_height == last_height: 
                        break
                        
                    last_height = new_height
            except selenium.common.exceptions.TimeoutException :
                self.logger.debug("error - get link - TimeoutException")
                time.sleep(delay_time)
                continue
                
            ##html source 추출   
            source = self.link_crawler.page_source
            soup = BeautifulSoup(source)
            
            ##게시물 링크 추출
            tag_a = soup.findAll('a',href=re.compile('/p/*/'))
            for tag in tag_a:
                self.save_link(tag["href"])
            
            cnt += len(tag_a)
            ##링크 추출 종료
            if cnt >= CRAWLING_COUNT : 
                break
        
        self.link_crawler.quit() ##링크 크롤러 종료
        self.logger.debug("debug - get link - link crawling complete")
                
    ##링크 파일 저장 함수
    def save_link(self,link):
        with self.lock:
            ##링크 중복검사
            df = pandas.read_csv(self.filepath_links,encoding='utf-8')
            df = df[df['link']==link]
            df = df[df['keyword']==self.keyword]    
            if df.empty == False: return

            ##링크 저장
            with open(self.filepath_links,'a',encoding='utf-8',newline='') as f:
                wr = csv.writer(f)
                wr.writerow([self.keyword,link,"no"])

            self.logger.debug("debug - get link - link save")

    ##링크 파일 삭제 함수
    def delete_link(self,link):
        with self.lock:
            link_data = pandas.read_csv(self.filepath_links,encoding='utf-8')
            new_data = link_data
        
            link_data = link_data[link_data['link'] == link]
            link_data = link_data[link_data['keyword']==self.keyword]
        
            new_data = new_data.drop(link_data.index[0],axis=0)
            new_data.to_csv(self.filepath_links,index = False,encoding='utf-8')
        self.logger.debug("error - connect page - link file delete : %s" % link)
        
        
    ##게시물 추출 체크 함수
    def check_get_content(self,link):
        with self.lock:
            link_data = pandas.read_csv(self.filepath_links,encoding='utf-8')
            new_data = link_data
            
            link_data = link_data[link_data['link']==link]
            link_data = link_data[link_data['keyword']==self.keyword]
            link_data = link_data[link_data['crawling']=='no']

            if link_data.empty == False :
                new_data.crawling.iloc[link_data.index[0]] = "yes"
                new_data.to_csv(self.filepath_links,index=False,encoding='utf-8')

     ##게시물 추출 함수
    def get_contents(self):
        crawling_contents_count = 0
        
        ##링크 불러옴
        with self.lock:
            df = pandas.read_csv(self.filepath_links,encoding='utf-8')
        df = df[df['keyword']==self.keyword]
        df = df[df['crawling']=='no']
        
        
        if df.empty == True :
            self.logger.debug("debug - get contents - No crawling contents data")
            return crawling_contents_count
        
        ##게시물 크롤링
        for link in df['link'] :
            content = [self.keyword]
            url = self.url+link
            res = self.connect_page(url,link)
                 
            if res != None :          
                ##게시물 추출
                soup = BeautifulSoup(res.text)
                content_text = soup.title.text.strip().split()
                content_text = ' '.join(content_text)
                content.append(content_text)
                
                ##태그 추출
                pattern = re.compile('#\w+')
                tags = pattern.findall(content_text)
                tags = ' '.join(tags)
                content.append(tags)
                
                ##날짜 추출
                try:
                    pattern = re.compile('\d{4}-\d{2}-\d{2}')
                    date = pattern.findall(res.text)[0]
                    content.append(date)
                except IndexError :
                    pass
                
                ##추출 게시물 저장
                self.save_content(content)
                self.check_get_content(link)
                
                crawling_contents_count += 1 ##게시물 저장 횟수
                
            else :
                self.logger.debug("error - get contents - page connect fail : None instance")
                continue
                
        return crawling_contents_count
                
    ##게시물 저장 함수
    def save_content(self,content):
        with self.lock:
            ##게시물 중복검사
            contents_data = pandas.read_csv(self.filepath_contents,encoding='utf-8')
            contents_data = contents_data[contents_data['text']==content[1]]
            contents_data = contents_data[contents_data['keyword']==content[0]]

            if contents_data.empty == False : return

            ##게시물 저장
            with open(self.filepath_contents,'a',encoding='utf-8',newline='') as f:
                wr = csv.writer(f)
                wr.writerow(content)
                
            self.logger.debug("debug - save content")
    
    ##새로운 링크 저장 확인 함수
    def is_new_link(self):
        with self.lock:
            link_data = pandas.read_csv(self.filepath_links,encoding='utf-8')
        link_data = link_data[link_data['keyword']==self.keyword]
        link_data = link_data[link_data['crawling']=="no"]
        if link_data.empty == False :
            return True
        return False

##슬랙설정
from slackclient import SlackClient

def notification(message,sc):
    sc.api_call(
        "chat.postMessage",
        channel="#crawling",
        text=message
    )

def is_user(k):
    return 'type' in k and 'text' in k and 'user' in k

def is_receive(data):
    return len(data)

##크롤링 함수
def crawling(keyword):
    crawler = InstagramCrawler()
    crawler.set_keyword(keyword)
    
    ##크롤러 병렬실행
    count = 0
    len_data = 0
    exe = ThreadPoolExecutor(max_workers=4)
    pool = exe.submit(crawler.get_links)
    while True:
        if not pool.done():
            len_data = crawler.get_contents()
            count = count + len_data
        else:
            if crawler.is_new_link():
                len_data = crawler.get_contents()
                count = count + len_data
            break
        time.sleep(0.1)
    
    return (keyword,count)

##슬랙연동 및 병렬실행
from concurrent.futures import ThreadPoolExecutor
if __name__ == '__main__':
    slack_token = 'xoxp-555943505926-555943507078-553777956800-ff7f2abaa95da316ea22bb0baab27b88'
    sc = SlackClient(slack_token)

    if sc.rtm_connect():
        while True:
            receive_data = sc.rtm_read()
            if is_receive(receive_data):
                keys = list(receive_data[0].keys())
                if is_user(keys):
                    message = receive_data[0]['text']
                    if message == 'crawler':
                        notification("크롤링 시작",sc)
                        
                        keywords = pandas.read_csv('./rawdata/category_dic.csv',encoding='utf-8')
                        for i in range(0,len(keywords)) : 
                            key, count = crawling(keywords.ix[i]['category'])
                            try:
                                notification("'%s'키워드 데이터 %d개 수집완료"%(key,count),sc)
                            except:
                                pass
                        notification("----크롤러 데이터 수집완료----",sc)
                            
                    elif message == 'exit':
                        notification("크롤러 종료",sc)
                        break
    else:
        print('Connection Failed')   

    

