import pandas as pd
### 데이터 불러들이기
# - (2024년 ~ 2022년 2월)
df = pd.read_csv('./news_comments_2024_2022.csv')

# cmt 컬럼 결측치를 <NULL> 로 변환
df['cmt'].fillna('<NULL>', inplace=True)

# cmt 컬럼 개행 제거
df['cmt'] = df['cmt'].str.replace('\n', '', regex=False)

# 파일 저장
df.to_csv('./news_comments_2024_2022_sep.csv', sep='\x01', index=False, header=False)

# - (2022년 ~ 2019년), 위 과정 반복
df2 = pd.read_csv('./news_comments_2022_2019.csv')
df2['cmt'].fillna('<NULL>', inplace=True)
df2['cmt'] = df2['cmt'].str.replace('\n', '', regex=False)
df2.to_csv('./news_comments_2022_2019_sep.csv', sep='\x01', index=False, header=False)

# - (2018년 ~ 2017년), 위 과정 반복
df3 = pd.read_csv('./news_comments_2018_2017.csv')
df3['cmt'].fillna('<NULL>', inplace=True)
df3['cmt'] = df3['cmt'].str.replace('\n', '', regex=False)
df3.to_csv('./news_comments_2018_2017_sep.csv', sep='\x01', index=False, header=False)