import pandas as pd
from bson import json_util
from pymongo import MongoClient, UpdateOne, DESCENDING, ASCENDING

from marcap_utils import marcap_data

pd.set_option('display.max_columns', None)
client = MongoClient("mongodb://root:admin123@localhost:27017/")
db = client["investing"]  # 데이터베이스 선택 혹은 생성
collection = db["stock_history"]  # 컬렉션 선택 혹은 생성
collection.create_index([("date", DESCENDING), ("code", ASCENDING)], unique=True)

for year in range(1995, 2024):
    df = marcap_data(f'{year}-01-01', f'{year}-12-31')
    df = df.drop(columns=["MarketId", "Rank", "Dept", "Changes", "ChangeCode"])
    df = df[df["Market"] != "KONEX"]
    df['Date'] = df.index.strftime("%Y-%m-%d")
    # 'Date' 열을 첫 번째 열로 재배치
    df = df[['Date'] + [col for col in df.columns if col != 'Date']]
    df = df.rename(columns={'ChagesRatio': 'change_rate'})
    df.columns = df.columns.str.lower()
    documents = json_util.loads(df.to_json(orient='records'))

    operations = []
    for row in documents:
        filter_ = {'date': row['date'], 'code': row['code']}
        update = {'$set': row}
        operations.append(UpdateOne(filter_, update, upsert=True))

    # Bulk Write 실행
    result = collection.bulk_write(operations)

    # collection.insert_many(documents)

    print(f"{year}년 데이터 저장 완료")
