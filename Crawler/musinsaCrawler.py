import os
import pandas
import csv
import time
import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
import selenium.common.exceptions 
from bs4 import BeautifulSoup
from threading import Lock
import logging

from crawler import Crawler

class MusinsaCrawler(Crawler):
    def __init__(self):
        Crawler.__init__(self)
        self.content_crawler = None
        ##상품평순
        ##self.url = "https://store.musinsa.com/app/product/search?search_type=1&pre_q=&d_cat_cd=&brand=&rate=&page_kind=search&list_kind=small&sort=emt_high&page=%s&display_cnt=120&sale_goods=&ex_soldout=&color=&popup=&q=%s&price1=&price2="
       
        ##낮은가격순
        self.url = "https://store.musinsa.com/app/product/search?search_type=1&pre_q=&d_cat_cd=&brand=&rate=&page_kind=search&list_kind=small&sort=price_low&page=%s&display_cnt=120&sale_goods=&ex_soldout=&color=&popup=&chk_research=&q=%s&chk_brand=&price1=&price2=&chk_color=&chk_sale=&chk_soldout="
        self.content_url = "https://store.musinsa.com"
                
        ##data디렉토리 및 파일 생성
        self.create_data_storage()
        
        ##로그설정
        Crawler.set_logs('Musinsa_Crawler_log','./logging/logfile_musinsa.log')

    ##팝업창 제거 함수
    def close_dialog_box(self):
        try:
            self.content_crawler.find_element_by_css_selector('#layer_timesale > div.box_btn > a.close').click()
        except:
            pass

        try:
            self.content_crawler.find_element_by_css_selector(
                '#page_product_detail > div.right_area.page_detail_product > div.popup_info.layer-suggest-join > div > div > a.day-popup-open').click()
        except:
            pass

        try:
            self.content_crawler.find_element_by_css_selector(
                '#divpop_goods_fatalism_3289 > form > a.pop-ntd').click()
        except:
            pass

    ##파일저장소 생성 함수
    def create_data_storage(self):
        ##데이터 저장폴더
        self.filepath_storage = './rawdata'
        if os.access(self.filepath_storage,os.F_OK) == False:
            os.mkdir(self.filepath_storage)

        ##링크파일 
        self.filepath_links = './rawdata/links_musinsa.csv'
        if os.access(self.filepath_links,os.F_OK) == False:
            new_data = {'keyword':[],'link':[],'crawling':[]}
            df = pandas.DataFrame(new_data,columns=['keyword','link','crawling'])
            df.to_csv(self.filepath_links,index=False, encoding='utf-8')
        
        ##리뷰파일
        self.filepath_contents = './rawdata/contents_musinsa.csv'
        if os.access(self.filepath_contents,os.F_OK) == False:
            new_data = {'keyword':[],'text':[],'date':[],'rank':[]}
            df = pandas.DataFrame(new_data,columns=['keyword','text','date','rank'])
            df.to_csv(self.filepath_contents,index=False, encoding='utf-8')
    
    def connecton(self,url,link=None):
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
        
        if res == None : return
        
        else :
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
        PAGE_COUNT = 2
        
        if self.keyword == None:
            return
        
        ##총페이지 추출
        res = self.connecton(self.url%(1,self.keyword))
        soup = BeautifulSoup(res.text)
        tag_span = soup.findAll("span",class_="totalPagingNum")
        try:
            total_page = int(tag_span[0].text.strip())
        except ValueError:                               ##예외처리
            total_page = (tag_span[0].text.strip()).split(",")
            total_page = "".join(total_page)
            total_page = int(total_page)
            
        ##페이지 이동
        for page in range(1,total_page):
            if page > PAGE_COUNT: 
                break
            
            ##페이지연결
            res = self.connecton(self.url%(page,self.keyword))  
            if res == None : continue
            
            ##링크추출
            soup = BeautifulSoup(res.text)
            tag_a = soup.findAll('a',href=re.compile('/app/product/detail/*'))
            for tag in tag_a:
                self.save_link(tag['href'])
        
        self.logger.debug("debug - get link - Complete link crawling ")
    
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
                
            self.logger.debug("debug - get link - Save link")

    ##컨텐츠 크롤러 페이지 접속함수        
    def connect_selenium(self,link):
        ##예외처리
        if self.content_crawler == None : ##크롬 드라이버 실행
            self.content_crawler=webdriver.Chrome(self.chrome_path,chrome_options=self.chromeOptions)
            
        url = self.content_url + link
        if self.connecton(url) != None:
            self.content_crawler.get(url)
            self.content_crawler.implicitly_wait(3)
        else:
            self.logger.debug("error - connect_selenium - None instance : connect link error")
            return False
        return True

    def contents_scrap(self,link):
        ##CSS 셀렉터 설정
        review_totalpage_selector = '#estimate_list > div > div > div.nslist_bottom > div.box_page_msg'
        pagebutton_selector = '#estimate_list > div > div > div.nslist_bottom > div.pagination.textRight > div > a:nth-child(%s)'
        nextpage_button_selector = '#estimate_list > div > div > div.nslist_bottom > div.pagination.textRight > div > a.fa.fa-angle-right.paging-btn.btn.next'
        review_selector = '#estimate_list > div > div > div:nth-child(%s) > div.postRight > div > div.pContent > div.summary > div > div.pContent_text > span'
        date_selector = '#estimate_list > div > div > div:nth-child(%s) > div.postRight > div > div.profile > p > span.date'
        rank_selector = '#estimate_list > div > div > div:nth-child(%s) > div.postRight > div > div.profile > p > span.img-score.score_%s0'
        
        delay_time = 0.8
        
        ##총 페이지 확인
        try:
            total_page= self.content_crawler.find_element_by_css_selector(review_totalpage_selector).text.split()[0]
        except selenium.common.exceptions.NoSuchElementException: ##후기가 없는 경우 
            self.check_get_content(link)
            return

        ##페이지 1,000개 넘으면 ','문자 처리해줌    
        try:
            total_page = int(total_page)
        except ValueError:
            total_page = total_page.split(",")
            total_page = "".join(total_page)
            total_page = int(total_page)

        self.content_crawler.execute_script("window.scrollTo(10000,0);") ##가로스크롤 이동

        for _ in range(0,total_page):  ##리뷰 페이지 반복
            for i in range(4,8):             ##리뷰 페이지 버튼
                for n in range(1,11):  ##리뷰 게시물 수
                    content = [self.keyword]
                    try:
                        ##리뷰 추출
                        review = self.content_crawler.find_element_by_css_selector(review_selector % str(n)).text
                        review = review.strip().split()
                        review = ' '.join(review)
                        content.append(review)

                        ##날짜 추출
                        date = self.content_crawler.find_element_by_css_selector(date_selector % str(n)).text
                        content.append(date)

                        for rank in range(5,0,-1):
                            try:
                                self.content_crawler.find_element_by_css_selector(rank_selector % (str(n),str(rank)))
                            except selenium.common.exceptions.NoSuchElementException:
                                pass
                            else:
                                ## 별점에 따라 리뷰긁어옴
                                #if rank < 3:
                                #    content.append(1)
                                #else:
                                #    content.append(-1)     
                                if rank <= 2:
                                    content.append(-1)
                                break

                        ##추출 게시물 저장
                        if(len(content) == 4):
                            self.save_content(content)
                        self.check_get_content(link)
                       
                        
                    except selenium.common.exceptions.NoSuchElementException: ##게시물 없으면 종료
                            return

                ##페이지 버튼으로 이동
                next_button = self.content_crawler.find_element_by_css_selector(nextpage_button_selector)
                ActionChains(self.content_crawler).move_to_element(next_button).perform()
                
                ##옆 페이지 버튼 클릭
                self.close_dialog_box()
                try:
                    self.content_crawler.find_element_by_css_selector(pagebutton_selector % str(i)).click()
                except selenium.common.exceptions.WebDriverException:
                    self.close_dialog_box()
                    self.content_crawler.find_element_by_css_selector(pagebutton_selector % str(i)).click()

                time.sleep(delay_time)
                
            ##다음 페이지 버튼 클릭
            self.close_dialog_box()
            try:
                self.content_crawler.find_element_by_css_selector(nextpage_button_selector).click()
            except selenium.common.exceptions.WebDriverException:
                self.close_dialog_box()
                self.content_crawler.find_element_by_css_selector(nextpage_button_selector).click()
            time.sleep(delay_time)
    
    ##게시물 추출 함수
    def get_contents(self):
        ##링크 불러옴
        with self.lock:
            df = pandas.read_csv(self.filepath_links,encoding='utf-8')
        df = df[df['keyword']==self.keyword]
        df = df[df['crawling']=='no']    
        
        if df.empty == True :
            self.logger.debug("debug - get contents - No crawling contents data")
            return 
        
        for link in df['link'] :
            res = self.connecton(self.content_url+link,link)
            if res == None : continue

            ##예외처리
            if self.connect_selenium(link) == False:
                continue
            try:
                self.contents_scrap(link)
            except selenium.common.exceptions.TimeoutException :
                self.logger.debug("error - get contents - TimeoutException")
                time.sleep(1)
            except selenium.common.exceptions.StaleElementReferenceException :
                self.logger.debug("error - get contents - StaleElementReferenceException")
            #except selenium.common.exceptions.WebDriverException :
                #self.logger.debug("error - get contents - WebDriverException")

        ##크롤러 종료
        self.content_crawler.quit()
        

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

    ##링크 파일 삭제 함수
    def delete_link(self,link):
        with self.lock:
            link_data = pandas.read_csv(self.filepath_links,encoding='utf-8')
            new_data = link_data
        
            link_data = link_data[link_data['link'] == link]
            link_data = link_data[link_data['keyword']==self.keyword]
        
            new_data = new_data.drop(link_data.index[0],axis=0)
            new_data.to_csv(self.filepath_links,index=False,encoding='utf-8')
        self.logger.debug("error - connect page - link file delete : %s" % link)

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
            
            self.logger.debug("debug - save_content - Save content")

    ##새로운 링크 저장 확인 함수
    def is_new_link(self):
        with self.lock:
            link_data = pandas.read_csv(self.filepath_links,encoding='utf-8')
        link_data = link_data[link_data['keyword']==self.keyword]
        link_data = link_data[link_data['crawling']=="no"]
        if link_data.empty == False :
            return True
        return False

if __name__ == "__main__":
    crawler = MusinsaCrawler()
    crawler.set_keyword('치노팬츠')
    crawler.get_links()
    crawler.get_contents()