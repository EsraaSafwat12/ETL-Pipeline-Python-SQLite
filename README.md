# ETL Pipeline - Python & SQLite 🔄

An ETL (Extract, Transform, Load) pipeline that processes 
student performance data using Python, Pandas, and SQLite.

## What is ETL?
- **Extract** – Read raw data from CSV file
- **Transform** – Clean, process, and enrich the data
- **Load** – Store the cleaned data into SQLite database

## Features
- Loads 1000+ student records from CSV
- Cleans missing values and duplicates
- Calculates total score, average score, and grade (A/B/C/D/F)
- Stores data in SQLite database
- Runs SQL queries for analysis:
  - Average scores by gender
  - Grade distribution
  - Top 5 students

## Technologies Used
- Python 3
- Pandas
- SQLite3
- CSV processing

## How to Run
```bash
pip install pandas
python etl_pipeline.py
```

## Results
- 1000 records processed
- Females scored higher in Reading & Writing
- Males scored higher in Math
- 3 students achieved perfect scores (100/100/100)

## Author
Esraa Safwat – Data Engineer | Computer Science Student
