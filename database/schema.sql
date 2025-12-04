-- Create database (run as postgres superuser or in psql)
-- CREATE DATABASE bank_reviews;

-- Connect to bank_reviews and run the following:

-- banks table
CREATE TABLE IF NOT EXISTS banks (
    bank_id SERIAL PRIMARY KEY,
    bank_code TEXT UNIQUE,
    bank_name TEXT NOT NULL,
    app_id TEXT,
    current_rating REAL,
    total_ratings INTEGER,
    total_reviews INTEGER
);

-- reviews table
CREATE TABLE IF NOT EXISTS reviews (
    review_id TEXT PRIMARY KEY,
    bank_id INTEGER REFERENCES banks(bank_id) ON DELETE CASCADE,
    review_text TEXT,
    rating SMALLINT,
    review_date DATE,
    review_year SMALLINT,
    review_month SMALLINT,
    user_name TEXT,
    thumbs_up INTEGER,
    text_length INTEGER,
    source TEXT,
    sentiment_label TEXT,
    sentiment_score REAL
);

-- Optional indices for faster queries
CREATE INDEX IF NOT EXISTS idx_reviews_bank_id ON reviews(bank_id);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);
CREATE INDEX IF NOT EXISTS idx_reviews_sentiment_score ON reviews(sentiment_score);
CREATE INDEX IF NOT EXISTS idx_reviews_review_date ON reviews(review_date);
