import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
import csv
from tqdm import tqdm
import uuid
import re

# 크롤링할 URL
base_url = "https://news.naver.com/main/list.naver?mode=LSD&mid=shm&date={}&sid1={}&page={}"

# 크롤링할 섹션 목록 (100: 정치, 101: 경제, 102: 사회, 103: 생활/문화, 104: 세계)
sections_to_crawl = [100, 101, 102, 103, 104]

# 크롤링할 기간 설정 (2017년 1월 1일부터 2024년 10월 20일까지)
start_date = datetime(2017, 1, 1)
end_date = datetime(2024, 10, 20)

# 최대 페이지 설정 (필요시 크롤링할 최대 페이지 수를 조정)
max_pages = 5

# 헤더 설정
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.59 Safari/537.36"
}

def generate_uuid():
    """UUID 형태의 고유 ID 생성"""
    return str(uuid.uuid1())

def is_korean_text(text):
    """텍스트가 한글을 포함하는지 확인"""
    return bool(re.search("[가-힣]", text))

def get_news_list(url, section):
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    
    # 기사 리스트 가져오기
    articles = soup.select(".list_body .type06_headline li, .list_body .type06 li")
    
    news_data = []
    
    # tqdm을 사용하여 게이지바 표시
    for article in tqdm(articles, desc=f"크롤링 진행중 (섹션: {section})", ncols=80):
        try:
            # 기사 제목 추출
            title_element = article.select_one("dt:not(.photo) a")  # 사진이 아닌 제목 링크 선택
            title = title_element.text.strip() if title_element else "제목을 가져올 수 없습니다."
            link = title_element["href"] if title_element else ""
            
            # 언론사 정보 추출
            media_com = article.select_one(".writing").text.strip() if article.select_one(".writing") else "언론사 정보 없음"
            
            # 기사 내용 크롤링
            article_data = get_article_content(link)
            
            # 영문 기사 제외: 제목과 본문 모두 한글이 포함된 경우만 수집
            if not is_korean_text(title) or not is_korean_text(article_data):
                continue
            
            # 고유 UUID 생성
            doc_id = generate_uuid()

            # 데이터 수집
            news_data.append({
                "doc_id": doc_id,
                "section": str(section),  # 섹션명을 설정
                "crawl_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "media_com": media_com,
                "title": title,
                "contents": article_data,
                "part_dt": datetime.now().strftime("%Y-%m-%d")
            })
        except Exception as e:
            print(f"Error occurred: {e}")

    return news_data

def get_article_content(url):
    if not url:
        return "내용을 가져올 수 없습니다."
    
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    
    # 수정된 기사 내용 크롤링 로직
    content_element = soup.select_one("#dic_area")  # 네이버 뉴스의 본문 영역은 '#dic_area' ID를 가짐
    contents = content_element.text.strip() if content_element else "내용을 가져올 수 없습니다."
    return contents

def save_to_csv(news_list, filename="naver_news.csv"):
    # 중복된 기사 내용 제거
    unique_news = []
    seen_contents = set()
    
    for news in news_list:
        if news['contents'] not in seen_contents:
            unique_news.append(news)
            seen_contents.add(news['contents'])
    
    # CSV 파일 저장
    keys = unique_news[0].keys()
    with open(filename, "w", newline="", encoding="utf-8-sig") as output_file:
        dict_writer = csv.DictWriter(output_file, fieldnames=keys)
        dict_writer.writeheader()
        dict_writer.writerows(unique_news)

def date_range(start, end):
    """시작 날짜와 종료 날짜 사이의 날짜 생성기"""
    delta = timedelta(days=1)
    current = start
    while current <= end:
        yield current
        current += delta

# 크롤링 실행
all_news = []
for section in sections_to_crawl:
    for single_date in date_range(start_date, end_date):
        formatted_date = single_date.strftime("%Y%m%d")
        for page in range(1, max_pages + 1):
            section_url = base_url.format(formatted_date, section, page)
            news_list = get_news_list(section_url, section)
            all_news.extend(news_list)

# 결과 CSV 파일로 저장
if all_news:
    save_to_csv(all_news)
    print("CSV 파일로 저장 완료: naver_news.csv")
else:
    print("크롤링된 데이터가 없습니다.")
