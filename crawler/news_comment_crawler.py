####################################################################
# Trino와 연결해서 사용하는 코드 수정해야 할 수 있음

# import requests
# import json
# from datetime import datetime
# import time
# import pandas as pd
# from tqdm import tqdm
# import re
# from trino.dbapi import connect

# class NaverNewsCommentCrawler:
#     def __init__(self, delay=1):
#         self.comments = []
#         self.delay = delay
#         self.cmt_ids = set()

#     def extract_oid_aid(self, url):
#         """기사 URL에서 oid와 aid를 추출"""
#         match = re.search(r'/article/(\d+)/(\d+)', url)
#         if not match:
#             # mnews 경로 처리
#             match = re.search(r'/mnews/article/(\d+)/(\d+)', url)

#         if match:
#             return match.group(1), match.group(2)  # oid, aid 반환
#         else:
#             raise ValueError(f"Invalid URL format: {url}")

#     def get_total_comments(self, oid, aid):
#         """기사의 총 댓글 수를 반환"""
#         header = {
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
#             'Referer': f'https://n.news.naver.com/article/{oid}/{aid}',
#         }
#         url = f"https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=news&templateId=view_society_m1&pool=cbox5&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&page=1&sort=FAVORITE"

#         try:
#             response = requests.get(url, headers=header)
#             response.raise_for_status()  # 네트워크 오류 시 예외 발생
#             json_data = response.text.strip()[response.text.find("(") + 1: -2]
#             comments_json = json.loads(json_data)
#             return comments_json['result']['count']['comment']
#         except (requests.RequestException, json.JSONDecodeError) as e:
#             print(f"Error while fetching total comments for oid={oid}, aid={aid}: {e}")
#             return 0

#     def get_comments(self, oid, aid, original_url, doc_id, delay=None, file_name="naver_news_comments_crawled.csv"):
#         """기사의 모든 댓글을 크롤링하며 실시간으로 CSV에 저장"""
#         total_comments = self.get_total_comments(oid, aid)
#         if total_comments == 0:
#             print(f"No comments for oid={oid}, aid={aid}")
#             return

#         header = {
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
#             'Referer': f'https://n.news.naver.com/article/{oid}/{aid}',
#         }

#         more_comments = True
#         more_param_next = None
#         comment_count = 0

#         # 파일에 첫 행을 기록 (컬럼 헤더 추가)
#         if not self.comments:  # 처음 실행할 때만 헤더를 기록
#             pd.DataFrame(columns=["doc_id", "cmt_id", "crawl_dt", "cmt_dt", "cmt", "url"]).to_csv(file_name, index=False)

#         with tqdm(total=total_comments, desc=f"Crawling comments for oid={oid}, aid={aid}") as pbar:
#             while more_comments:
#                 try:
#                     if more_param_next:
#                         c_url = f"https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=news&templateId=default_society&pool=cbox5&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&moreParam.next={more_param_next}&sort=FAVORITE&pageType=more"
#                     else:
#                         c_url = f"https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=news&templateId=default_society&pool=cbox5&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&sort=FAVORITE&initialize=true"

#                     response = requests.get(c_url, headers=header)
#                     response.raise_for_status()
#                     json_data = response.text.strip()[response.text.find("(") + 1: -2]
#                     comments_json = json.loads(json_data)

#                     if 'result' in comments_json and 'commentList' in comments_json['result']:
#                         comments_list = comments_json['result']['commentList']
#                         if not comments_list:
#                             break

#                         for comment in comments_list:
#                             cmt_id = comment['commentNo']
#                             if cmt_id in self.cmt_ids:
#                                 continue

#                             self.cmt_ids.add(cmt_id)
#                             comment_data = {
#                                 "doc_id": doc_id,  # 기사와 동일한 doc_id 사용
#                                 "cmt_id": cmt_id,
#                                 "crawl_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                                 "cmt_dt": comment['regTime'],
#                                 "cmt": comment['contents'],
#                                 "url": original_url  # 원본 기사 URL 추가
#                             }
#                             self.comments.append(comment_data)

#                             # 실시간으로 CSV 파일에 기록
#                             pd.DataFrame([comment_data]).to_csv(file_name, mode='a', header=False, index=False)

#                         comment_count += len(comments_list)
#                         pbar.update(len(comments_list))
#                         time.sleep(delay or self.delay)

#                         more_param_next = comments_json['result'].get('morePage', {}).get('next')

#                         if comment_count >= total_comments or not more_param_next:
#                             more_comments = False
#                     else:
#                         more_comments = False

#                 except (requests.RequestException, json.JSONDecodeError) as e:
#                     print(f"Error while fetching comments: {e}")
#                     more_comments = False

#     def crawl_news_comments(self, news_data, file_name="naver_news_comments_crawled.csv"):
#         """기사 데이터 목록에서 URL과 doc_id로 모든 댓글 크롤링"""
#         for _, row in news_data.iterrows():
#             try:
#                 url = row['url']
#                 doc_id = row['doc_id']
#                 oid, aid = self.extract_oid_aid(url)
#                 self.get_comments(oid, aid, url, doc_id, file_name=file_name)
#             except ValueError as e:
#                 print(e)

#     def fetch_urls_from_trino(self, query, host='opyter.iptime.org', port=40000, user='airflow', catalog='dl_iceberg', schema='stg'):
#         """Trino에서 URL을 가져오는 함수"""
#         try:
#             conn = connect(
#                 host=host,
#                 port=port,
#                 user=user,
#                 catalog=catalog,
#                 schema=schema
#             )
#             cur = conn.cursor()
#             cur.execute(query)
#             rows = cur.fetchall()
#             return [row[0] for row in rows]
#         except Exception as e:
#             print(f"Error while fetching URLs from Trino: {e}")
#             return []


# if __name__ == "__main__":
#     # Trino에서 URL 가져오기
#     query = """SELECT url
#                FROM dl_iceberg.ods.ds_naver_news
#             """
#     crawler = NaverNewsCommentCrawler()
#     news_urls = crawler.fetch_urls_from_trino(query)

#     # 뉴스 댓글 크롤링 실행
#     crawler.crawl_news_comments(news_urls)

#     # CSV 파일로 저장
#     crawler.save_to_csv("naver_news_comments_crawled.csv")

import requests
import json
from datetime import datetime
import time
import pandas as pd
from tqdm import tqdm
import re
from trino.dbapi import connect


class NaverNewsCommentCrawler:
    def __init__(self, delay=1):
        self.comments = []
        self.delay = delay
        self.cmt_ids = set()

    def extract_oid_aid(self, url):
        """기사 URL에서 oid와 aid를 추출"""
        match = re.search(r'/article/(\d+)/(\d+)', url)
        if not match:
            # mnews 경로 처리
            match = re.search(r'/mnews/article/(\d+)/(\d+)', url)

        if match:
            return match.group(1), match.group(2)  # oid, aid 반환
        else:
            raise ValueError(f"Invalid URL format: {url}")

    def get_total_comments(self, oid, aid):
        """기사의 총 댓글 수를 반환"""
        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Referer': f'https://n.news.naver.com/article/{oid}/{aid}',
        }
        url = f"https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=news&templateId=view_society_m1&pool=cbox5&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&page=1&sort=FAVORITE"

        try:
            response = requests.get(url, headers=header)
            response.raise_for_status()  # 네트워크 오류 시 예외 발생
            json_data = response.text.strip()[response.text.find("(") + 1: -2]
            comments_json = json.loads(json_data)
            return comments_json['result']['count']['comment']
        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"Error while fetching total comments for oid={oid}, aid={aid}: {e}")
            return 0

    def get_comments(self, oid, aid, original_url, doc_id, delay=None, file_name="news_comments_2022_2019.csv"):
        """기사의 모든 댓글을 크롤링하며 실시간으로 CSV에 저장"""
        total_comments = self.get_total_comments(oid, aid)
        if total_comments == 0:
            print(f"No comments for oid={oid}, aid={aid}")
            return

        header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Referer': f'https://n.news.naver.com/article/{oid}/{aid}',
        }

        more_comments = True
        more_param_next = None
        comment_count = 0

        # 파일에 첫 행을 기록 (컬럼 헤더 추가)
        if not self.comments:  # 처음 실행할 때만 헤더를 기록
            pd.DataFrame(columns=["doc_id", "cmt_id", "crawl_dt", "cmt_dt", "cmt", "url"]).to_csv(file_name,
                                                                                                  index=False)

        with tqdm(total=total_comments, desc=f"Crawling comments for oid={oid}, aid={aid}") as pbar:
            while more_comments:
                try:
                    if more_param_next:
                        c_url = f"https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=news&templateId=default_society&pool=cbox5&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&moreParam.next={more_param_next}&sort=FAVORITE&pageType=more"
                    else:
                        c_url = f"https://apis.naver.com/commentBox/cbox/web_naver_list_jsonp.json?ticket=news&templateId=default_society&pool=cbox5&lang=ko&country=KR&objectId=news{oid}%2C{aid}&pageSize=20&sort=FAVORITE&initialize=true"

                    response = requests.get(c_url, headers=header)
                    response.raise_for_status()
                    json_data = response.text.strip()[response.text.find("(") + 1: -2]
                    comments_json = json.loads(json_data)

                    if 'result' in comments_json and 'commentList' in comments_json['result']:
                        comments_list = comments_json['result']['commentList']
                        if not comments_list:
                            break

                        for comment in comments_list:
                            cmt_id = comment['commentNo']
                            if cmt_id in self.cmt_ids:
                                continue

                            self.cmt_ids.add(cmt_id)
                            comment_data = {
                                "doc_id": doc_id,  # 기사와 동일한 doc_id 사용
                                "cmt_id": cmt_id,
                                "crawl_dt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                "cmt_dt": comment['regTime'],
                                "cmt": comment['contents'],
                                "url": original_url  # 원본 기사 URL 추가
                            }
                            self.comments.append(comment_data)

                            # 실시간으로 CSV 파일에 기록
                            pd.DataFrame([comment_data]).to_csv(file_name, mode='a', header=False, index=False)

                        comment_count += len(comments_list)
                        pbar.update(len(comments_list))
                        time.sleep(delay or self.delay)

                        more_param_next = comments_json['result'].get('morePage', {}).get('next')

                        if comment_count >= total_comments or not more_param_next:
                            more_comments = False
                    else:
                        more_comments = False

                except (requests.RequestException, json.JSONDecodeError) as e:
                    print(f"Error while fetching comments: {e}")
                    more_comments = False

    def crawl_news_comments(self, news_data, file_name="news_comments_2022_2019.csv"):
        """기사 데이터 목록에서 URL과 doc_id로 모든 댓글 크롤링"""
        start_date = datetime(2019, 1, 1)
        end_date = datetime(2022, 2, 2)

        for _, row in news_data.iterrows():
            try:
                publish_date_str = row.get('publish_date')
                if not publish_date_str or "알 수 없음" in publish_date_str:  # publish_date가 없거나 '알 수 없음'이면 건너뜀
                    continue

                # 시간 정보가 포함된 날짜도 처리하도록 수정
                try:
                    publish_date = datetime.strptime(publish_date_str.split()[0], "%Y-%m-%d")
                except ValueError:
                    print(f"Invalid date format for {publish_date_str}, skipping.")
                    continue

                # 기사 작성일이 2019년 1월 1일부터 2022년 2월 2일 사이인지 확인
                if start_date <= publish_date <= end_date:
                    url = row['url']
                    doc_id = row['doc_id']
                    oid, aid = self.extract_oid_aid(url)
                    self.get_comments(oid, aid, url, doc_id, file_name=file_name)
            except (ValueError, KeyError) as e:
                print(f"Error processing row: {e}")

    def fetch_urls_from_trino(self, query, host='opyter.iptime.org', port=40000, user='airflow', catalog='dl_iceberg',
                              schema='stg'):
        """Trino에서 URL을 가져오는 함수"""
        try:
            conn = connect(
                host=host,
                port=port,
                user=user,
                catalog=catalog,
                schema=schema
            )
            cur = conn.cursor()
            cur.execute(query)
            rows = cur.fetchall()
            return [row[0] for row in rows]
        except Exception as e:
            print(f"Error while fetching URLs from Trino: {e}")
            return []


if __name__ == "__main__":
    # 네이버 기사 데이터를 불러와 doc_id와 url을 함께 사용
    news_data = pd.read_csv("naver_news_103_239.csv")
    crawler = NaverNewsCommentCrawler()
    crawler.crawl_news_comments(news_data, file_name="news_comments_2022_2019.csv")


