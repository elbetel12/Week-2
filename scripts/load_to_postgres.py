"""
Usage:
export DB_URL="postgresql+psycopg2://username:password@localhost:5432/bank_reviews"
python scripts/load_to_postgres.py /path/to/bank_reviews_clean.csv
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError

DB_URL = os.getenv("DB_URL")
print(f"Using DB_URL: {DB_URL}")
if not DB_URL:
    raise SystemExit("Set DB_URL env var, e.g. postgresql+psycopg2://user:pass@localhost:5432/bank_reviews")

csv_path = sys.argv[1] if len(sys.argv) > 1 else "../notebooks/data/processed/reviews_processed.csv"
df = pd.read_csv(csv_path, dtype=str)  # read as str first to avoid dtype issues

# normalize columns: adapt if necessary
df = df.rename(columns={
    'review_id':'review_id',
    'review_text':'review_text',
    'rating':'rating',
    'review_date':'review_date',
    'review_year':'review_year',
    'review_month':'review_month',
    'bank_code':'bank_code',
    'bank_name':'bank_name',
    'user_name':'user_name',
    'thumbs_up':'thumbs_up',
    'text_length':'text_length',
    'source':'source',
    # if sentiment columns exist:
    'sentiment':'sentiment_label',
    'sentiment_score':'sentiment_score'
})

# Ensure expected columns exist
expected = ['review_id','review_text','rating','review_date','review_year','review_month',
            'bank_code','bank_name','user_name','thumbs_up','text_length','source',
            'sentiment_label','sentiment_score']
for c in expected:
    if c not in df.columns:
        # fill missing optional columns with nulls
        df[c] = None

# Convert numeric/date types
df['rating'] = pd.to_numeric(df['rating'], errors='coerce').astype('Int64')
df['thumbs_up'] = pd.to_numeric(df['thumbs_up'], errors='coerce').astype('Int64')
df['text_length'] = pd.to_numeric(df['text_length'], errors='coerce').astype('Int64')
df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce').dt.date
df['review_year'] = pd.to_numeric(df['review_year'], errors='coerce').astype('Int64')
df['review_month'] = pd.to_numeric(df['review_month'], errors='coerce').astype('Int64')
df['sentiment_score'] = pd.to_numeric(df['sentiment_score'], errors='coerce')

engine = create_engine(DB_URL, pool_pre_ping=True)

with engine.begin() as conn:
    # Ensure tables already created (optional)
    conn.execute(text(open("database/schema.sql").read()))

    # Upsert banks: get mapping bank_code -> bank_id
    banks_df = df[['bank_code','bank_name']].drop_duplicates().copy()
    # If app-level metadata exists in another source, you can add app_id/current_rating etc.

    for _, row in banks_df.iterrows():
        bank_code = row['bank_code']
        bank_name = row['bank_name']
        if pd.isna(bank_code) and pd.isna(bank_name):
            continue
        # insert if not exists
        upsert_sql = text("""
        INSERT INTO banks (bank_code, bank_name)
        VALUES (:bank_code, :bank_name)
        ON CONFLICT (bank_code) DO UPDATE SET bank_name = EXCLUDED.bank_name
        RETURNING bank_id
        """)
        try:
            res = conn.execute(upsert_sql, {"bank_code": bank_code, "bank_name": bank_name})
        except Exception as e:
            # fallback: try insert without bank_code unique conflict (if bank_code null)
            if pd.isna(bank_code):
                # try to insert by name if name not exists
                res = conn.execute(text("""
                    INSERT INTO banks (bank_name) 
                    SELECT :bank_name
                    WHERE NOT EXISTS (SELECT 1 FROM banks WHERE bank_name = :bank_name)
                    RETURNING bank_id
                """), {"bank_name": bank_name})
            else:
                raise

    # Build bank_name => bank_id mapping
    bank_map = {r['bank_name']: r['bank_id'] for r in conn.execute(text("SELECT bank_id, bank_name FROM banks")).mappings()}

    # Prepare reviews dataframe for insert
    insert_rows = []
    for _, r in df.iterrows():
        bank_id = bank_map.get(r['bank_name'])
        if bank_id is None:
            # if bank_name not found, skip or insert new bank
            res = conn.execute(text("INSERT INTO banks (bank_name) VALUES (:name) RETURNING bank_id"), {"name": r['bank_name']})
            bank_id = res.scalar()
            bank_map[r['bank_name']] = bank_id

        insert_rows.append({
            "review_id": r['review_id'],
            "bank_id": bank_id,
            "review_text": r['review_text'],
            "rating": int(r['rating']) if pd.notna(r['rating']) else None,
            "review_date": r['review_date'],
            "review_year": int(r['review_year']) if pd.notna(r['review_year']) else None,
            "review_month": int(r['review_month']) if pd.notna(r['review_month']) else None,
            "user_name": r['user_name'],
            "thumbs_up": int(r['thumbs_up']) if pd.notna(r['thumbs_up']) else None,
            "text_length": int(r['text_length']) if pd.notna(r['text_length']) else None,
            "source": r['source'],
            "sentiment_label": r['sentiment_label'],
            "sentiment_score": float(r['sentiment_score']) if pd.notna(r['sentiment_score']) else None
        })

    # Bulk insert (chunked)
    chunk_size = 1000
    for i in range(0, len(insert_rows), chunk_size):
        chunk = insert_rows[i:i+chunk_size]
        conn.execute(
            text("""
            INSERT INTO reviews (
                review_id, bank_id, review_text, rating, review_date, review_year, review_month,
                user_name, thumbs_up, text_length, source, sentiment_label, sentiment_score
            ) VALUES (
                :review_id, :bank_id, :review_text, :rating, :review_date, :review_year, :review_month,
                :user_name, :thumbs_up, :text_length, :source, :sentiment_label, :sentiment_score
            )
            ON CONFLICT (review_id) DO NOTHING
            """),
            chunk
        )

print("Done: rows inserted.")
