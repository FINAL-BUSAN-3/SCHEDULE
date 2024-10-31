import pandas as pd
import multiprocessing
from konlpy.tag import Okt
from collections import Counter

# Okt 초기화
okt = Okt()
exclude_word = ['이', '가', '은', '는', '을', '를', '에', '의', '하다', '있다', '되다', '현상', '발생일'] #뺄 단어(불용어)
#include_word = ['미션', '엔진', '배터리', '퓨즈', '전기', '센서', '에어컨', '브레이크'] # 고장관련 키워드중 많은걸 찾을 때 (예 : ('엔진', 6330), ('배터리', 1673) )

# 형태소 분석 함수 정의 (명사 추출)불용어 처리까지
def analyze_nouns(text):
    nouns = okt.nouns(text)
    filtered_nouns = [noun for noun in nouns if noun not in exclude_word]
   # must_nouns = [noun for noun in nouns if noun in exclude_word]
    return filtered_nouns if pd.notnull(text) else []

# 병렬 처리 함수
def parallel_morph_analysis(data, num_workers=4):
    with multiprocessing.Pool(num_workers) as pool:
        results = pool.map(analyze_nouns, data)
    return results

# CSV 파일 불러오기
if __name__ == '__main__':
    csv_file_path = './data/defect_report22.06~24.10.19.csv' # 파일넣어서 만듬
    df = pd.read_csv(csv_file_path, delimiter='\t', engine='python', on_bad_lines='warn') #read_sql로 DB에서 뽑아와서 해야할듯

    # 형태소 분석 대상 열 선택
    texts = df['결함내용'].tolist() # 결함내용 컬럼, 컬럼명넣기

    # 형태소 분석 실행
    morphed_texts = parallel_morph_analysis(texts)

    # 명사들을 한 리스트로 합치기
    all_nouns = [noun for nouns_list in morphed_texts for noun in nouns_list]

    # 명사 빈도수 계산
    noun_counter = Counter(all_nouns)

    # 결과 출력
    print(noun_counter.most_common()) # 명사 출력중(많은순으로)