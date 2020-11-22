import pandas
import csv
import multiprocessing

##언급량분석기

class MentionAnalysis():
    def __init__(self):
        self.MEDIA_MUSINSA = "musinsa"
        self.MEIDA_INSTA = "insta"
        self.filepath = None
        self.date_pattern = None
        self.keyword = None
        self.media = None
        self.year = None
        self.months = ["01","02","03","04","05","06","07","08","09","10","11","12"]

    ##키워드 설정 함수
    def set_Keyword(self,keyword):
        self.keyword = keyword
    
    ##매체 설정 함수          --> 여기서 데이터셋 폴더경로를 설정해주어야 함
    def set_Media(self,media):
        self.media = media
        if self.media == self.MEIDA_INSTA:
            self.filepath='./Crawler/rawdata/contents_instagram.csv'
            self.date_pattern = "%s-%s-"
        elif self.media == self.MEDIA_MUSINSA:
            self.filepath='./Crawler/rawdata/contents_musinsa.csv'
            self.date_pattern = "%s.%s."
        else :
            self.media = None
    
    ##연도설정 함수
    def set_Year(self,year):
        self.year = year

    ##언급량수 계산 함수 -> 입력받은 달에 해당하는 언급량
    def get_mention_for_month(self,month) :
        month = str(month)
        if len(month) == 1: 
            month = "0"+month
        if month not in self.months : raise Exception

        ##날짜 패턴 설정
        pattern = self.date_pattern % (self.year,month)

        ##패턴에 매칭되는 값으로 필터링
        df = pandas.read_csv(self.filepath,index_col=False)
        df = df.dropna(how="any")
        df = df.ix[df['date'].str.startswith(pattern),:]
        total = len(df)
        df = df.ix[df['text'].str.find(self.keyword)>-1,:]
        
        return (month,len(df),total)

    #언급량수 계산 함수 -> 설정된 연도에 해당하는 언급량
    def get_mention_for_year(self):
        ##병렬처리계산 
        pool = multiprocessing.Pool(processes = 4)
        result = pool.map(self.get_mention_for_month,self.months)
        pool.close()
        pool.join()

        return result


import time
if __name__ == '__main__':
    analysis = MentionAnalysis()
    analysis.set_Keyword("오버핏")  ##키워드설정
    analysis.set_Media(analysis.MEDIA_MUSINSA)  ##매체설정 -> instagram/musinsa
    analysis.set_Year("2018")      ##연도설정

    ##월별 언급량 출력함수  --> 결과값은 튜플 형태 --> (달,언급량,월별전체리뷰수)
    print(analysis.get_mention_for_month("01"))
    print(analysis.get_mention_for_month("02"))
    print(analysis.get_mention_for_month("03"))
    print(analysis.get_mention_for_month("04"))
    print(analysis.get_mention_for_month("05"))
    print(analysis.get_mention_for_month("06"))
    print(analysis.get_mention_for_month("07"))
    print(analysis.get_mention_for_month("08"))
    print(analysis.get_mention_for_month("09"))
    print(analysis.get_mention_for_month("10"))
    print(analysis.get_mention_for_month("11"))
    print(analysis.get_mention_for_month("12"))

    ##연도별 언급량 출력함수 --> 결과값은 튜플 리스트 형태 -->[(달,언급량,월별전체리뷰수),.....]
    print(analysis.get_mention_for_year())


    


    




    