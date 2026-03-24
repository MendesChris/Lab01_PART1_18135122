import psycopg2
import os


base_dir = os.path.dirname(os.path.dirname(__file__))
output_path = os.path.join(base_dir, "Data", "raw", "taxi_trips.csv")

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="12571", #insert postgres password for execution
    host="localhost",
    port="5432"
)

query = "SELECT * FROM public.taxi_trips"

with conn:
    with conn.cursor() as cur:
        with open(output_path, "w", encoding="utf-8") as f:
            cur.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", f)

conn.close()

print("Bronze layer loaded successfully!")