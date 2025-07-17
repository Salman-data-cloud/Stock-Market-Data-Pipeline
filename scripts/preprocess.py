import pandas as pd 
import os  
from dateutil.parser import parse
from datetime import datetime

def clean_stock_data():
    

    raw_path = '/opt/airflow/datasets/raw_datasets'

    all_files = [f for f in os.listdir(raw_path) if f.endswith('.csv')]
    standard_col_order = ['Date', 'Scrip', 'Open', 'High', 'Low', 'Close', 'Volume']
    missing_header_order = ['Scrip', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume']

    all_df = []
    for file in all_files:

        try:
            full_path = os.path.join(raw_path, file)
            #print("Processing file:", full_path)
            with open(full_path, 'r', encoding = 'utf=8') as f:
                first_line = f.readline().strip().split(',')
                if set(standard_col_order).issubset(set(first_line)):
                    df = pd.read_csv(full_path)
                else:
                    df = pd.read_csv(full_path, header=None, names = missing_header_order)

                # To Ensure consistency in column order
                df = df[standard_col_order]
                df['source_file'] = file
                #print(f"Loaded: {file} | Date range: {df['Date'].min()} to {df['Date'].max()}")
                all_df.append(df)
        except Exception as e:
            print(f"Error processing file {file}: {e}")

    combined_df = pd.concat(all_df, ignore_index= True)

    def date_parser(date_val):
        try:
            if isinstance(date_val, str) and date_val.isdigit() and len(date_val) ==8:
                return pd.to_datetime(date_val, format = '%Y%m%d', errors = 'coerce')

            elif isinstance(date_val, (int,float)) and len(str(int(date_val))) ==8:
                return pd.to_datetime(str(int(date_val)), format = '%Y%m%d', errors = 'coerce')
            else:
                return parse(str(date_val), dayfirst=True)
        except Exception as e:
            return pd.NaT
        

    combined_df['Date'] = combined_df['Date'].apply(date_parser)
    combined_df = combined_df.sort_values('Date').reset_index(drop=True)
    #print(combined_df['Date'].dtype)

    nat_count = combined_df['Date'].isna().sum()
    #print(f'Number of NaT values in Date column: {nat_count}')
    combined_df = combined_df.dropna(subset=['Date'])
    combined_df.reset_index(drop=True, inplace = True)


    combined_df['Open'] = pd.to_numeric(combined_df['Open'], errors='coerce')
    combined_df['Low'] = pd.to_numeric(combined_df['Low'], errors='coerce')
    combined_df['Volume'] = pd.to_numeric(combined_df['Volume'], errors='coerce')
    combined_df.dropna(inplace = True)

    combined_df =combined_df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
    combined_df['Scrip'] = combined_df['Scrip'].str.strip().str.upper()

    combined_df = combined_df.reset_index(drop=True)

    print(combined_df.dtypes)
    print(combined_df.isna().sum())
    print(combined_df.nunique())
    summary = pd.DataFrame({
        'Data type': combined_df.dtypes,
        'Null Count': combined_df.isna().sum(),
        'Unique values': combined_df.nunique()
    })
    print(summary)

    duplicate_rows = combined_df.duplicated().sum()
    print(f"Number of duplicate rows: {duplicate_rows}")
    combined_df = combined_df.drop_duplicates().reset_index(drop=True)

    invalid_price_rows = combined_df[(combined_df['Low']>combined_df['High']) |
                                    (combined_df['Open']>combined_df['High']) |
                                    (combined_df['Low']>combined_df['Open']) |
                                    (combined_df['Close']>combined_df['High'])] 
    print(f"Number of rows with invalid prices: {len(invalid_price_rows)}")

    negative_values = combined_df[
        (combined_df['Open'] < 0) |
        (combined_df['High'] < 0) |
        (combined_df['Low'] < 0) |
        (combined_df['Close'] < 0) |
        (combined_df['Volume'] < 0)
    ]

    print(f"Number of rows with negative values: {len(negative_values)}")

    combined_df = combined_df.drop(invalid_price_rows.index).reset_index(drop=True)

    zero_volume_count = (combined_df['Volume'] == 0).sum()
    print(f"Zero volume rows: {zero_volume_count}")
    combined_df = combined_df[combined_df['Volume']>0].reset_index(drop=True)

    print(combined_df.shape)
    print(combined_df.describe())
    print(combined_df.head(5))
    print(combined_df.tail(5))

    cleaned_path = '/opt/airflow/datasets/cleaned_dataset'
    os.makedirs(cleaned_path, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f'dse_cleaned_1999_2023_{timestamp}.csv'
    output_path = os.path.join(cleaned_path, filename)
    combined_df.to_csv(output_path, index=False)
    print("Preprocessing complete. Cleaned data saved to:", output_path)
    # End of preprocessing script
    print("Script execution finished.")

if __name__ == "__main__":
    clean_stock_data()
