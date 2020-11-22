import requests

if __name__ == '__main__':
    keyword = '치노팬츠'   ##검색 키워드
    media = 'musinsa'  ##크롤링할 매체

    url =  'http://127.0.0.1:5000/%s/%s' % (media,keyword) 
    res = requests.get(url)   ##GET 요청
    print(res)