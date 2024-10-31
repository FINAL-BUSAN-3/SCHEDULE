import requests
import json
from datetime import datetime
import time
import pandas as pd
from tqdm import tqdm
import re

class NaverNewsCommentCrawler:
    def __init__(self, delay=1):
        self.comments = []
        self.delay = delay  # 크롤링 속도를 조절하기 위한 delay
        self.cmt_ids = set()  # 이미 수집된 댓글 ID를 추적하기 위한 set

    def extract_oid_aid(self, url):
        """기사 URL에서 oid와 aid를 추출"""
        # URL에서 기사 번호 추출
        match = re.search(r'/article/(\d+)/(\d+)', url)
        if match:
            return match.group(1), match.group(2)  # oid, aid 반환
        else:
            raise ValueError(f"Invalid URL format: {url}")

    def get_total_comments(self, oid, aid):
        """기사의 총 댓글 수를 반환"""
        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Referer': f'https://n.news.naver.com/article/{oid}/{aid}',
            'Accept': '*/*',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
        }

        url = f"https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=news&templateId=view_society_m1&pool=cbox5&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&page=1&sort=FAVORITE"
        response = requests.get(url, headers=header)
        if response.status_code != 200:
            print(f"Failed to fetch comment data for oid={oid}, aid={aid}")
            return 0

        try:
            json_data = response.text.strip()[response.text.find("(") + 1: -2]
            comments_json = json.loads(json_data)
            total_comments = comments_json['result']['count']['comment']
            return total_comments

        except json.JSONDecodeError as e:
            print(f"Error while parsing total comments: {e}")
            return 0

    def get_comments(self, oid, aid):
        """기사의 모든 댓글을 크롤링"""
        total_comments = self.get_total_comments(oid, aid)
        if total_comments == 0:
            print(f"No comments for oid={oid}, aid={aid}")
            return

        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Referer': f'https://n.news.naver.com/article/{oid}/{aid}',
            'Accept': '*/*',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Connection': 'keep-alive',
        }

        print(f"Total comments: {total_comments}")
        more_comments = True
        last_comment_id = None  # 첫 페이지는 None
        more_param_next = None  # 다음 페이지의 'next' 값 저장
        comment_count = 0

        with tqdm(total=total_comments, desc=f"Crawling comments for oid={oid}, aid={aid}") as pbar:
            while more_comments:
                try:
                    if more_param_next:
                        c_url = f"https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=news&templateId=default_society&pool=cbox5&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&moreParam.next={more_param_next}&sort=FAVORITE&pageType=more"
                    else:
                        c_url = f"https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=news&templateId=default_society&pool=cbox5&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&sort=FAVORITE&initialize=true"

                    response = requests.get(c_url, headers=header)
                    if response.status_code != 200:
                        print(f"Failed to fetch comments for oid={oid}, aid={aid}")
                        break

                    json_data = response.text.strip()[response.text.find("(") + 1: -2]
                    comments_json = json.loads(json_data)

                    if 'result' in comments_json and 'commentList' in comments_json['result']:
                        comments_list = comments_json['result']['commentList']
                        if not comments_list:
                            print(f"No more comments on this page.")
                            break

                        for comment in comments_list:
                            cmt_id = comment['commentNo']
                            if cmt_id in self.cmt_ids:
                                continue

                            self.cmt_ids.add(cmt_id)
                            self.comments.append({
                                "doc_id": aid,
                                "cmt_id": cmt_id,
                                "crawl_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "cmt_dt": comment['regTime'],
                                "cmt": comment['contents'],
                            })

                        comment_count += len(comments_list)
                        pbar.update(len(comments_list))
                        time.sleep(self.delay)

                        more_param_next = comments_json['result']['morePage']['next']

                        if comment_count >= total_comments or not more_param_next:
                            more_comments = False

                    else:
                        print(f"No result found.")
                        more_comments = False

                except Exception as e:
                    print(f"Error: {e}")
                    more_comments = False

    def crawl_news_comments(self, news_urls):
        """뉴스 기사 URL 목록에서 모든 댓글 크롤링"""
        for url in news_urls:
            try:
                oid, aid = self.extract_oid_aid(url)
                self.get_comments(oid, aid)
            except ValueError as e:
                print(e)

    def save_to_csv(self, file_name):
        """크롤링한 댓글을 CSV로 저장"""
        df = pd.DataFrame(self.comments)
        df.to_csv(file_name, index=False)
        print(f"Saved {len(self.comments)} comments to {file_name}")


if __name__ == "__main__":
    # naver_news_comments.csv 파일에서 'url' 컬럼 데이터 읽어오기
    df = pd.read_csv("naver_news.csv")

    # 데이터프레임의 'url' 컬럼을 사용하여 크롤링할 URL 리스트 만들기
    news_urls = df['url'].tolist()

    # 뉴스 댓글 크롤링 실행
    crawler = NaverNewsCommentCrawler()
    crawler.crawl_news_comments(news_urls)

    # CSV 파일로 저장
    crawler.save_to_csv("naver_news_comments_crawled.csv")
