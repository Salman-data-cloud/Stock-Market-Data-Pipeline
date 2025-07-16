import os
import shutil

source_paths =[
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\1999-2021',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\july',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\june',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\march',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\may',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\nov',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\oct',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\april',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\aug',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\dec',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\feb',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2022\jan',
    r'C:\Users\salma\Downloads\archive (17)\stock_market\data(99-23)\2023'
]

target_raw_folder = r'C:\Users\salma\stock_market_de_project\datasets\raw_datasets'

for path in source_paths:
    if os.path.exists(path):
        for file in os.listdir(path):
            if file.endswith('.csv'):
                full_file_path = os.path.join(path,file)
                shutil.copy(full_file_path, target_raw_folder)
                print(f'Copied file from {full_file_path} to {target_raw_folder}')

print('All files have been copied successfully.')