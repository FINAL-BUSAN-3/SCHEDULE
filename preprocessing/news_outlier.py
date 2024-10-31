import pandas as pd
# 데이터 불러들이기
df = pd.read_csv('./naver_news_103_239.csv')

# section 103으로 변환
df['section'] = 103

# title, contents 컬럼의 개행 제거
df['title'] = df['title'].str.replace('\n', '', regex=False)
df['contents'] = df['contents'].str.replace('\n', '', regex=False)

# 파일 저장
df.to_csv('./naver_news_103_239_sep.csv', sep='\x01', index=False, header=False)
