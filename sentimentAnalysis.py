import pandas
import csv
import pickle
import numpy
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression

class SentimentAnalysis():
    def __init__(self):
        self.filepath = "./data/"
        self.modelFile = "tf_idf_model.pkl"
        self.preproFile = "preprocessed.csv"
        self.model = None
        self.keyword = None
        
        self.load_model()
        
    ##감정분석 학습 모델 로드
    def load_model(self):
        with open(self.filepath+self.modelFile,'rb') as f:
            self.model = pickle.load(f)

    ##전처리 데이터 로드
    def load_preprocess_data(self,keyword):
        ##전처리 데이터 불러오고 필터링
        data = pandas.read_csv(self.filepath+self.preproFile)
        data = data[data['keyword']==self.keyword]
        review_data = data['text']
        rank_data = data['rank']

        ##리뷰 데이터와 랭크 데이터로 분리
        review_data = [str(text) for text in review_data]
        rank_data = [int(rank) for rank in rank_data]

        return (review_data,rank_data)

    ##긍/부정 예측
    def predict_sentiment(self,text):
        example = [text]

        pred = self.model.predict(example)[0]
        probability = numpy.max(self.model.predict_proba(example))*100

        return (int(pred),probability)


    def set_keyword(self,keyword):
        self.keyword = keyword

    def get_keyword(self):
        return self.keyword
        
if __name__ == '__main__':
    analysis = SentimentAnalysis()
    analysis.set_keyword('셔츠')

    label = {-1:"부정적",1:"긍정적"}
    print("please enter review")
    while True:
        print("\t",end=" ")
        review = input()

        pred,prob = analysis.predict_sentiment(review)
        print("\t",label[pred],prob,"%")
        print()

    
    
