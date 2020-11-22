from flask import Flask
from instagramCrawler import InstagramCrawler
from musinsaCrawler import MusinsaCrawler

##크롤러서버
    ##GET방식으로 통신. URL에 검색할 키워드 데이터를 넘겨줌
    ##서버IP주소 다음에 크롤러 선택
    ##<data> 이 부분에 검색할 키워드가 들어감
    ##ex) http://서버IP주소/musinsa/청바지

app = Flask(__name__)

##인스타그램 크롤러
@app.route('/instagram/<data>' ,methods=['GET'])
def crawling_instagram(data):

    crawler = InstagramCrawler()
    crawler.set_keyword(data)
    crawler.get_links()
    crawler.get_contents()       
    return data

##무신사 크롤러
@app.route('/musinsa/<data>' ,methods=['GET'])
def crawling_musinsa(data):
    
    crawler = MusinsaCrawler()
    crawler.set_keyword(data)
    crawler.get_links()
    crawler.get_contents()

    return data

if __name__ == "__main__":
    app.run()