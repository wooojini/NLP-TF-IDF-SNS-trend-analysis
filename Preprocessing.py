from konlpy.tag import Okt
import csv
import pandas
import os
import time
import re
import logging
import pickle
import json
from threading import Thread
import jpype

class Preprocessor():
    def __init__(self):
        self.keyword = None
        self.media = None
        self.MEDIA_MUSINSA = "musinsa"
        self.MEDIA_INSTAGRAM = "insta"

        ##파일 경로
        self.folder_filepath = './data'
        self.prepro_filename = '/preprocessed.csv'
        self.stop_words_filename = '/stopword.csv'
        self.raw_musinsa_filepath = './Crawler/rawdata/contents_musinsa.csv'
        self.raw_insta_filepath = './Crawler/rawdata/contents_instagram.csv'
        
        self.logger = self.settingLogger()

        ##전처리 데이터 폴더 생성
        if os.access(self.folder_filepath,os.F_OK) == False:
            os.mkdir(self.folder_filepath)

        ## 전처리 데이터 파일 생성
        filepath = self.folder_filepath + self.prepro_filename
        if os.access(filepath,os.F_OK) == False:
            new_data = {'keyword':[],'text':[],'rank':[],'date':[]}
            df = pandas.DataFrame(new_data,columns=['keyword','text','rank','date'])
            df.to_csv(filepath,index=False, encoding='utf-8')
        
    def settingLogger(self):
        # 로거 생성
        logger = logging.getLogger('preprocessing.log')
        logger.setLevel(logging.DEBUG)

        # 로그포맷팅
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # 스트림 핸들러
        streamHandler = logging.StreamHandler()
        streamHandler.setLevel(logging.DEBUG)
        streamHandler.setFormatter(formatter)

        logger.addHandler(streamHandler)

        return logger
    
    def set_keyword(self,keyword):
        self.keyword = keyword
    
    def get_keyword(self,keyword):
        return self.keyword

    def set_media(self,media):
        self.media = media
    
    def get_media(self,media):
        return self.media

    def excute_preprocessing(self):
        ##예외처리
        if(self.keyword == None):
            print("Please set keyword")
            return
        if(self.media == None):
            print("Please set media")
            return 

        # stopword ---> 나중에 인스타도 불용어 추가해야함 
        filepath = self.folder_filepath + self.stop_words_filename
        df = pandas.read_csv(filepath,index_col=False, encoding='utf-8')
        stop_words = list(df['word'])

        ##raw data 파일경로 설정
        if(self.media == self.MEDIA_MUSINSA):
            filepath = self.raw_musinsa_filepath
        elif(self.media == self.MEDIA_INSTAGRAM):
            filepath = self.raw_insta_filepath

        # raw data
        raw_data = pandas.read_csv(
            filepath, index_col=False, encoding='utf-8')
        raw_data = raw_data[raw_data['keyword'] == self.keyword]
        #raw_data = raw_data[raw_data.index <= 100]  # 데이터 수 조절
        #raw_data

        ##스레드 작업 범위 설정
        start = raw_data.index[0]
        end = raw_data.index[-1]
        nlines = len(raw_data)

        ##전처리 작업 병렬 수행
        okt = Okt()
        result = [] ##전처리 결과
        try:
            self.logger.debug("Preprocessing Start - Data length : %s" % nlines)
            t1 = Thread(target=self.preprocessing_parallel,
                        args=(start, start+int(nlines/2), raw_data, okt, result, True, stop_words))
            t2 = Thread(target=self.preprocessing_parallel, args=(
                start+int(nlines/2), end, raw_data, okt, result, True, stop_words))
            t1.start()
            t2.start()
            t1.join()
            t2.join()
        except KeyboardInterrupt:
            pass
        except Exception:
            pass
        finally:
            self.save_data(result)
            self.logger.debug("Preprocessing Complete")
            self.logger.debug("\t Keyword %s" % self.keyword)
            self.logger.debug("\t Complete data length %s" % len(result))

    # 병렬 데이터 전처리
    def preprocessing_parallel(self,start, end, raw_data, okt, result, remove_stopwords=False, stop_words=[]):
        # 함수의 인자는 다음과 같다.
        # 병렬 수행함으로 작업 처리 범위 인덱스 지정
            # start : 전처리할 로우 데이터 시작 인덱스
            # end : 전처리할 로우 데이터 마지막 인덱스
        # review : 로우 데이터.
        # okt : okt 객체를 반복적으로 생성하지 않고 미리 생성후 인자로 받는다.
        # result : 전처리 결과 저장할 리스트.
        # remove_stopword : 불용어를 제거할지 선택 기본값은 False
        # stop_word : 불용어 사전은 사용자가 직접 입력해야함 기본값은 비어있는 리스트

        jpype.attachThreadToJVM()

        #전처리할 텍스트 한글 및 공백 문자 제외 모두 제거
        review_text = raw_data['text']
        review_text = [okt.morphs(re.sub("[^가-힣ㄱ-ㅎㅏ-ㅣ\\s]", "", review_text[i]))
                    for i in range(start, end)]
        
        # okt 객체를 활용해서 형태소 단위로 나눈다.
        index = start;
        if remove_stopwords:
            # 불용어 제거(선택적)
            for review in review_text:
                word_review = [
                    token for token in review if not token in stop_words]
                word_review = ' '.join(word_review)
                result.append([self.keyword,word_review,raw_data['rank'].ix[index],raw_data['date'].ix[index]])
                index+=1
        return
    
    ##전처리 데이터 파일 저장
    def save_data(self,result):
        self.logger.debug("Save data")
        filepath = self.folder_filepath + self.prepro_filename
        with open(filepath,'a',encoding="utf-8",newline='') as f:
            wr = csv.writer(f)
            for data in result:
                wr.writerow(data)

        df = pandas.read_csv(filepath,encoding='utf-8',index_col=False)
        df.drop_duplicates().to_csv(filepath,index=False,encoding='utf-8')

if __name__ == '__main__':
    prepro = Preprocessor()
    prepro.set_keyword("맨투맨") ##나중에는 지워야 할 것
    prepro.set_media(prepro.MEDIA_MUSINSA)

    start_time = time.time()
    prepro.excute_preprocessing()
    print("time : ", (time.time()-start_time))
