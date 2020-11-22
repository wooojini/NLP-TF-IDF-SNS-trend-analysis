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
        self.lock = Lock()
        self.chrome_path = 'C:\chromedriver_win32\chromedriver.exe' 
        
        ##크롬옵션 설정
        self.set_chromeOptions()
        
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

    ##크롬드라이버 옵션 설정
    def set_chromeOptions(self):
        self.chromeOptions = Options()
        self.chromeOptions.add_argument('--ignore-certificate-errors')  #SSL 관련 오류 무시
        self.chromeOptions.add_argument('--ignore-ssl-errors')  #SSL 관련 오류 무시
    
    ##로그 파일 설정
    @classmethod
    def set_logs(self,logger_name,filepath):
        ##로깅파일 생성
        if os.access('./logging',os.F_OK) == False:
            os.mkdir('./logging')   
        
        ##로거 생성
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(logging.DEBUG)
        
        ##로그포맷팅
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        
        ##파일 핸들러
        fileHandler = logging.FileHandler(filepath)
        fileHandler.setLevel(logging.DEBUG)
        fileHandler.setFormatter(formatter)
        
        ##스트림 핸들러
        streamHandler = logging.StreamHandler()
        streamHandler.setLevel(logging.DEBUG)
        streamHandler.setFormatter(formatter)
        
        ##핸들러 등록
        self.logger.addHandler(fileHandler)
        self.logger.addHandler(streamHandler)
    
