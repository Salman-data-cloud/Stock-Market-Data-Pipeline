import os
import pandas as pd
from snowflake.connector import ProgrammingError
from snowflake_config import get_snowflake_connection

CHUNK_SIZE = 50000

def load_cleaned_data_to_snowflake():
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    folder_path = "./datasets/cleaned_dataset"
    table_name = "STOCK_CLEANED"

    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            file_path = os.path.join(folder_path, filename)
            df = pd.read_csv(file_path)

            df.rename(columns={
                'Date': 'TradingDate',
                'Scrip': 'Scrip',
                'Open': 'OpenPrice',
                'High': 'HighPrice',
                'Low': 'LowPrice',
                'Close': 'ClosePrice',
                'Volume': 'Volume',
                'source_file': 'SourceFile'
            }, inplace=True)

            # Fixed: auto parse datetime
            df['TradingDate'] = pd.to_datetime(df['TradingDate']).dt.date

            df.dropna(subset=['TradingDate', 'Scrip'], inplace=True)
            df.drop_duplicates(subset=['TradingDate', 'Scrip'], inplace=True)

            columns = list(df.columns)
            placeholders = "(" + ",".join(["%s"] * len(columns)) + ")"

            rows = df.to_records(index=False).tolist()
            print(f"Inserting {len(rows)} rows from {filename}...")

            for i in range(0, len(rows), CHUNK_SIZE):
                chunk = rows[i:i+CHUNK_SIZE]
                try:
                    cursor.executemany(
                        f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({','.join(['%s']*len(columns))})",
                        chunk
                    )
                    print(f"Inserted chunk rows {i} to {i+len(chunk)-1}")
                except ProgrammingError as e:
                    print(f"Error inserting chunk {i}-{i+len(chunk)-1} from {filename}: {e}")
                    continue
            
            print(f"Finished processing {filename}")
            break 
    cursor.close()
    conn.close()

if __name__ == "__main__":
    load_cleaned_data_to_snowflake()
    print("Data loading completed.")
