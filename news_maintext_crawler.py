"""생활/문화-자동차/시승기"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import csv
from tqdm import tqdm
import uuid
import re
import time

# 크롤링할 URL (자동차/시승기 카테고리)
base_url = "https://news.naver.com/main/list.naver?mode=LS2D&mid=sec&sid1=103&sid2=239&date={}&page={}"

# 크롤링할 기간 설정
start_date = datetime(2024, 10, 20)
end_date = datetime(2017, 1, 1)

# 최대 페이지 설정 (필요시 크롤링할 최대 페이지 수를 조정)
max_pages = 5

# 세션 설정 및 재시도 로직 추가
session = requests.Session()
retry_strategy = Retry(
    total=3,  # 최대 3번 재시도
    backoff_factor=2,  # 재시도 간격을 지수적으로 증가시키기 (2초, 4초, 8초...)
    status_forcelist=[500, 502, 503, 504],  # 재시도할 HTTP 상태 코드
    allowed_methods=["GET"]  # 재시도할 HTTP 메소드
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)


def generate_uuid():
    """UUID 형태의 고유 ID 생성"""
    return str(uuid.uuid1())


def is_korean_text(text):
    """텍스트가 한글을 포함하는지 확인"""
    return bool(re.search("[가-힣]", text))


def contains_keywords(text):
    """특정 키워드가 텍스트에 포함되어 있는지 확인"""
    keywords = ["현대차", "현대자동차", "기아", "기아차", "기아자동차", "車"]
    return any(keyword in text for keyword in keywords)


def get_news_list(url, writer, counter):
    try:
        response = session.get(url, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # 기사 리스트 가져오기
    articles = soup.select(".list_body .type06_headline li, .list_body .type06 li")

    # tqdm을 사용하여 게이지바 표시
    for article in tqdm(articles, desc=f"크롤링 진행중 (자동차/시승기)", ncols=80):
        try:
            # 기사 제목 추출
            title_element = article.select_one("dt:not(.photo) a")  # 사진이 아닌 제목 링크 선택
            title = title_element.text.strip() if title_element else "제목을 가져올 수 없습니다."
            link = title_element["href"] if title_element else ""

            # 언론사 정보 추출
            media_com = article.select_one(".writing").text.strip() if article.select_one(".writing") else "언론사 정보 없음"

            # 기사 내용 크롤링
            article_data, publish_date = get_article_content(link)

            # 영문 기사 제외 및 키워드 필터링
            if (not is_korean_text(title) or not is_korean_text(article_data)) or (
                    not contains_keywords(title) and not contains_keywords(article_data)):
                continue

            # 고유 UUID 생성
            doc_id = generate_uuid()

            # 데이터 실시간으로 저장
            writer.writerow({
                "doc_id": doc_id,
                "section": "자동차/시승기",
                "crawl_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "media_com": media_com,
                "title": title,
                "contents": article_data,
                "url": link,
                "publish_date": publish_date,
                "part_dt": datetime.now().strftime("%Y-%m-%d")
            })

            # 카운터 증가 및 출력
            counter[0] += 1
            print(f"현재 저장된 데이터 개수: {counter[0]}")
        except Exception as e:
            print(f"Error occurred: {e}")


def get_article_content(url, max_retries=3):
    """지정된 URL에서 기사 내용을 가져오되, HTTP 500 오류가 지속적으로 발생하면 건너뜀"""
    retries = 0
    while retries < max_retries:
        try:
            response = session.get(url, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
            break  # 요청 성공 시 반복 종료
        except requests.RequestException as e:
            print(f"Error fetching article {url}: {e}")
            retries += 1
            time.sleep(2)  # 재시도 전 잠시 대기

    if retries == max_retries:
        print(f"Skipping URL due to too many failed attempts: {url}")
        return "내용을 가져올 수 없습니다.", "알 수 없음"

    soup = BeautifulSoup(response.text, "html.parser")

    # 기사 본문 내용 크롤링
    content_element = soup.select_one("#dic_area")
    contents = content_element.text.strip() if content_element else "내용을 가져올 수 없습니다."

    # 기사 작성일 크롤링
    publish_date_element = soup.select_one(".media_end_head_info_datestamp_time")
    publish_date = publish_date_element["data-date-time"] if publish_date_element else "알 수 없음"

    return contents, publish_date


def date_range(start, end):
    """시작 날짜와 종료 날짜 사이의 날짜 생성기"""
    delta = timedelta(days=1)
    current = start
    while current >= end:
        yield current
        current -= delta

if __name__ == "__main__":



    # 1.
    #   dictionary 선언
    #   크롤링할 기간 설정 (logical_date)
    #   max_pages = 5
    #   f"https://news.naver.com/main/list.naver?mode=LS2D&mid=sec&sid1=103&sid2=239&date={formatted_date}&page={page}"
    #   dictionary -> csv
    # 2. hdfs put
    # 3. csv delete
    # 4. hive table repair
    # 5. Trino iceberg



    document_all = {}
    document = {}
    """
    writer.writerow({
        "doc_id": doc_id,
        "section": "자동차/시승기",
        "crawl_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "media_com": media_com,
        "title": title,
        "contents": article_data,
        "url": link,
        "publish_date": publish_date,
        "part_dt": datetime.now().strftime("%Y-%m-%d")
    })
    """

    document['section'] = "자동차/시승기",
    document['crawl_dt'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

    document_all[generate_uuid()] = document

    # 크롤링 실행 및 실시간 저장
    with open("naver_news_103_239.csv", "w", newline="", encoding="utf-8") as output_file:
        fieldnames = ["doc_id", "section", "crawl_dt", "media_com", "title", "contents", "url", "publish_date", "part_dt"]
        writer = csv.DictWriter(output_file, fieldnames=fieldnames)
        writer.writeheader()

        counter = [0]  # 저장된 데이터 개수를 추적하기 위한 리스트

        for single_date in date_range(start_date, end_date):  # 최근 날짜부터 크롤링
            formatted_date = single_date.strftime("%Y%m%d")
            for page in range(1, max_pages + 1):

                section_url = base_url.format(formatted_date, page)
                # get_news_list(section_url, writer, counter)

                """
                try:
                    response = session.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    response.raise_for_status()
                except requests.RequestException as e:
                    print(f"Error fetching URL {url}: {e}")
                    return None
            
                soup = BeautifulSoup(response.text, "html.parser")
            
                # 기사 리스트 가져오기
                articles = soup.select(".list_body .type06_headline li, .list_body .type06 li")
            
                # tqdm을 사용하여 게이지바 표시
                for article in tqdm(articles, desc=f"크롤링 진행중 (자동차/시승기)", ncols=80):
                    try:
                        # 기사 제목 추출
                        title_element = article.select_one("dt:not(.photo) a")  # 사진이 아닌 제목 링크 선택
                        title = title_element.text.strip() if title_element else "제목을 가져올 수 없습니다."
                        link = title_element["href"] if title_element else ""
            
                        # 언론사 정보 추출
                        media_com = article.select_one(".writing").text.strip() if article.select_one(".writing") else "언론사 정보 없음"
            
                        # 기사 내용 크롤링
                        article_data, publish_date = get_article_content(link)
            
                        # 영문 기사 제외 및 키워드 필터링
                        if (not is_korean_text(title) or not is_korean_text(article_data)) or (
                                not contains_keywords(title) and not contains_keywords(article_data)):
                            continue
            
                        # 고유 UUID 생성
                        doc_id = generate_uuid()
            
                        # 데이터 실시간으로 저장
                        writer.writerow({
                            "doc_id": doc_id,
                            "section": "자동차/시승기",
                            "crawl_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "media_com": media_com,
                            "title": title,
                            "contents": article_data,
                            "url": link,
                            "publish_date": publish_date,
                            "part_dt": datetime.now().strftime("%Y-%m-%d")
                        })
            
                        # 카운터 증가 및 출력
                        counter[0] += 1
                        print(f"현재 저장된 데이터 개수: {counter[0]}")
                    except Exception as e:
                        print(f"Error occurred: {e}")
                """

    print("크롤링 및 CSV 파일로 실시간 저장 완료.")
