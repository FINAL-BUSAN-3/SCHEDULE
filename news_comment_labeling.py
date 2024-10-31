import pandas as pd

# 파일 로드
df = pd.read_csv('./news_comments_2024_2022_sep.csv')
df2 = pd.read_csv('./news_comments_2022_2019_sep.csv')
df3 = pd.read_csv('./news_comments_2018_2017_sep.csv')

# 전체 데이터 합치기
df_combine = pd.concat([df, df2, df3])

# 누락된 값 제거
df_clean = df_combine[df_combine['cmt'] != '<NULL>'].reset_index(drop=True)

# 라벨링을 위해 cmt 컬럼만 사용
df_clean_cmt = df_clean['cmt']

# pre_training을 위해 10만개 추출, 300개의 데이터 수기로 라벨링
df_clean_cmt_1 = df_clean_cmt.iloc[:100000]

# 0:부정, 1:긍정
labels = [0,0,0,0,1,0,1,0,1,0,0,0,0,1,0,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,1,0,1,0,0,0,1,0,0,0,1,0,0,1,0,0,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,0,1,1,0,1,0,1,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,1,0,1,1,0,0,0,0,0,0,1,1,0,0,0,1,0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,0,0,0,1,0,0,0,0,0,0,0,0,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]

# label 컬럼 추가
# 첫 300개 행에 label 컬럼 추가
df_clean_cmt_1.loc[:299, 'label'] = labels
pre_labeled = df_clean_cmt_1.iloc[:300, ]

# 파일 저장
df_clean_cmt.to_csv('df_clean_cmt.txt', sep='\t', index=False, encoding='utf-8')
df_clean_cmt_1.to_csv('df_clean_cmt_1.csv', sep='\t', index=False, encoding='utf-8')
pre_labeled.to_csv('df_labeled.csv', sep='\t', index=False, encoding='utf-8')

### 이후 모델링은 구글 코랩에서 진행
from google.colab import files
# df_labeled.csv 파일 업로드
uploaded = files.upload()

# 라이브러리 설치 및 파일 로드
!pip install transformers datasets pandas
!pip install konlpy

from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline, Trainer, TrainingArguments

from datasets import Dataset
from konlpy.tag import Okt
from transformers import Trainer, TrainingArguments, DataCollatorWithPadding, BertTokenizer, BertForSequenceClassification
from torch.utils.data import Dataset, DataLoader
import torch
import pandas as pd
from sklearn.model_selection import train_test_split
import re
from torch.utils.data import Dataset
import pandas as pd
from sklearn.utils.class_weight import compute_class_weight
import numpy as np
data = pd.read_csv("df_labeled.csv",delimiter='\t', encoding='utf-8', on_bad_lines='skip')  # 300개 라벨링한 데이터 파일

# GPU 사용 가능 여부 확인 후 디바이스 설정
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 형태소 분석기 초기화
okt = Okt()

# 불용어 리스트 정의
stopwords = ["정말", "매우", "그", "으로", "를", "하", "아", "의", "가", "이", "은", "들", "는", "좀",
             "잘", "걍", "과", "도", "자",
             "에", "와", "한", "하다"]

# 형태소 분석 및 불용어 제거 함수 정의
def preprocess_text(text, stopwords):
    # 한글과 공백만 남기기
    text = re.sub(r"[^ㄱ-ㅎㅏ-ㅣ가-힣 ]", "", text)

    # 형태소 분석 후 품사 태깅
    morphs = okt.pos(text, stem=True)

    # 명사와 동사 추출 및 불용어 제거
    filtered_words = [
        word for word, pos in morphs
        if (pos in ["Noun", "Verb"]) and (word not in stopwords)
    ]

    # 결과를 하나의 문자열로 반환
    return " ".join(filtered_words)


# 1. 초기 데이터 로드 및 전처리
data['label'] = data['label'].astype(int)  # float형 라벨을 int형으로 변환

# 긍정 데이터 샘플링하여 부정 데이터 수와 맞춤
negative_data = data[data['label'] == 0]
positive_data = data[data['label'] == 1].sample(len(negative_data), replace=True, random_state=42)
balanced_data = pd.concat([positive_data, negative_data])

# 현대 및 기아 관련 댓글 필터링
# data = data[data['cmt'].str.contains("현대|기아|현기|아반떼|가격|현대차|기아차|")]

# 전처리 함수 적용
balanced_data['cmt'] = balanced_data['cmt'].apply(lambda x: preprocess_text(x, stopwords))

# 학습 및 검증 데이터 분리
train_texts, val_texts, train_labels, val_labels = train_test_split(
    data["cmt"], data["label"], test_size=0.2, random_state=42
)

# 토크나이저와 모델 설정
tokenizer = BertTokenizer.from_pretrained("monologg/kobert")
model = BertForSequenceClassification.from_pretrained("monologg/kobert", num_labels=2)

# 강력한 클래스 가중치 설정
from sklearn.utils.class_weight import compute_class_weight

class_weights = compute_class_weight('balanced', classes=np.unique(balanced_data['label']), y=balanced_data['label'])
class_weights = torch.tensor(class_weights * 5, dtype=torch.float).to(device)  # 가중치 배수 설정

# Custom Dataset 정의
class CustomDataset(Dataset):
    def __init__(self, texts, labels):
        self.encodings = tokenizer(list(texts), truncation=True, padding=True, max_length=128, return_tensors="pt")
        self.labels = torch.tensor(labels.values)

    def __getitem__(self, idx):
        item = {key: val[idx] for key, val in self.encodings.items()}
        item["labels"] = self.labels[idx]
        return item

    def __len__(self):
        return len(self.labels)

# Dataset 생성
train_dataset = CustomDataset(train_texts, train_labels)
val_dataset = CustomDataset(val_texts, val_labels)

# 데이터 콜레이터 설정 (자동 패딩)
data_collator = DataCollatorWithPadding(tokenizer=tokenizer)

class WeightedTrainer(Trainer):
    def compute_loss(self, model, inputs, return_outputs=False):
        labels = inputs.get("labels")
        outputs = model(**inputs)
        logits = outputs.get("logits")

        # 가중치가 적용된 손실 함수
        loss_fct = torch.nn.CrossEntropyLoss(weight=class_weights)
        loss = loss_fct(logits.view(-1, self.model.config.num_labels), labels.view(-1))

        return (loss, outputs) if return_outputs else loss

# 모델 학습 설정 및 Trainer
training_args = TrainingArguments(
    output_dir='./results', num_train_epochs=50, per_device_train_batch_size=64, logging_dir='./logs', report_to="none"
)
# WeightedTrainer로 변경하여 학습
trainer = WeightedTrainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    data_collator=data_collator,
)

# 모델 학습
trainer.train()

# 모델 가중치 저장
import os
import torch
from transformers import BertForSequenceClassification

# 모델 저장 경로 설정
model_dir = r"C:\Users\kfq\bs_20240520"
model_path = os.path.join(model_dir, "label_model.pth")

# 디렉토리가 없으면 생성
os.makedirs(model_dir, exist_ok=True)

# 모델의 상태_dict 저장
torch.save(model.state_dict(), model_path)
print(f"모델이 '{model_path}' 경로에 저장되었습니다.")

### 저장한 모델 로드
import torch
from transformers import BertForSequenceClassification

# 모델 경로를 올바르게 설정
model_path = r"/content/C:\Users\kfq\bs_20240520/label_model.pth"

# 모델 구조 생성 (저장된 모델과 동일한 구조 사용)
model = BertForSequenceClassification.from_pretrained("monologg/kobert", num_labels=2)

# 저장된 가중치 로드
model.load_state_dict(torch.load(model_path))
model.to(device)  # GPU 또는 CPU로 이동
print("모델이 성공적으로 로드되었습니다.")

model.eval()

tokenizer = BertTokenizer.from_pretrained("monologg/kobert")

# 전체 댓글 데이터 파일 업로드
uploaded = files.upload()

from torch.utils.data import DataLoader, Dataset

# 라벨링할 데이터 불러오기 및 전처리
unlabeled_data = pd.read_csv("df_clean_cmt.csv", delimiter='\t', encoding='utf-8', on_bad_lines='skip')
unlabeled_texts = unlabeled_data["cmt"].apply(lambda x: preprocess_text(x, stopwords))


# Dataset 정의
class UnlabeledDataset(Dataset):
    def __init__(self, texts):
        self.texts = texts

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        return self.texts[idx]


# DataLoader 생성
dataset = UnlabeledDataset(unlabeled_texts)
dataloader = DataLoader(dataset, batch_size=32)  # 배치 크기 32 설정

# 라벨링 수행 (배치별 예측)
all_predictions = []

with torch.no_grad():
    for batch_texts in dataloader:
        # 배치 텍스트 토큰화 후 GPU로 이동
        encodings = tokenizer(list(batch_texts), truncation=True, padding=True, max_length=128, return_tensors="pt").to(
            device)

        # 모델 예측
        outputs = model(**encodings)
        predictions = torch.argmax(outputs.logits, dim=-1).cpu().numpy()
        all_predictions.extend(predictions)

# 예측 결과를 데이터에 추가
unlabeled_data['label'] = all_predictions
print(unlabeled_data.head())

# 라벨링 처리한 데이터 저장 경로
output_path = "/content/C:/Users/kfq/bs_20240520/labeled_cmt.csv"

# 라벨링 데이터 저장 (인덱스를 제외하고 저장, 구분자는 \x01로 설정)
unlabeled_data.to_csv(output_path, index=False, encoding='utf-8-sig', sep='\x01')
print(f"라벨링된 데이터가 '{output_path}' 경로에 저장되었습니다.")
