import pandas as pd
import sqlite3
import os


# ===== EXTRACT =====
def extract(filepath):
    print(" Extracting data...")
    df = pd.read_csv(filepath)
    print(f" Loaded {len(df)} records")
    return df


# ===== TRANSFORM =====
def transform(df):
    print("\n Transforming data...")

    df = df.dropna()

    df = df.drop_duplicates()

    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')


    score_cols = ['math_score', 'reading_score', 'writing_score']
    df['total_score'] = df[score_cols].sum(axis=1)
    df['average_score'] = df[score_cols].mean(axis=1).round(2)

    df['grade'] = df['average_score'].apply(lambda x:
                                            'A' if x >= 90 else
                                            'B' if x >= 80 else
                                            'C' if x >= 70 else
                                            'D' if x >= 60 else 'F')

    print(f" Transformed {len(df)} records")
    return df


# ===== LOAD =====
def load(df, db_name="students.db"):
    print("\n Loading data into SQLite...")
    conn = sqlite3.connect(db_name)
    df.to_sql('students_performance', conn, if_exists='replace', index=False)
    conn.close()
    print(f" Data loaded into {db_name}")


# ===== SQL QUERIES =====
def analyze(db_name="students.db"):
    print("\n Running SQL Queries...")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print("\n Average scores by gender:")
    cursor.execute("""
        SELECT gender, 
               ROUND(AVG(math_score), 2) as avg_math,
               ROUND(AVG(reading_score), 2) as avg_reading,
               ROUND(AVG(writing_score), 2) as avg_writing
        FROM students_performance
        GROUP BY gender
    """)
    for row in cursor.fetchall():
        print(row)

    print("\n Grade distribution:")
    cursor.execute("""
        SELECT grade, COUNT(*) as count
        FROM students_performance
        GROUP BY grade
        ORDER BY grade
    """)
    for row in cursor.fetchall():
        print(row)

    print("\n Top 5 students:")
    cursor.execute("""
        SELECT * FROM students_performance
        ORDER BY average_score DESC
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(row)

    conn.close()


# ===== MAIN =====
if __name__ == "__main__":
    filepath = "StudentsPerformance.csv"
    df = extract(filepath)
    df = transform(df)
    load(df)
    analyze()
    print("\n ETL Pipeline completed successfully!")