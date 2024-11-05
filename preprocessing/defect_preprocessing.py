import pandas as pd
import numpy as np

if __name__ == "__main__":
    df = pd.read_csv("data/defect_report.csv", sep="\x01", header=None)

    need_manufacturer = (df[8] == '기아 주식회사') | (df[8] == '현대자동차(주)')
    df = df[need_manufacturer]

    # 모든 값이 null값인 행 찾아서 삭제
    all_nan_or_empty_df = df.isnull().all(axis=1) | (df == ' ').all(axis=1)
    df.drop(df[all_nan_or_empty_df].index, inplace=True)

    # partdt null 채우기
    g = df[14].isna() == True
    df[14].fillna(df[g][5].str[:7], inplace=True)
    df = df.reset_index(drop=True)
    df.to_csv("processed_defect.csv", header=False)