import os
import shutil
from pathlib import Path

def import_files():
    source_paths =[
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '1999-2021',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'july',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'june',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'march',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'may',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'nov',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'oct',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'april',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'aug',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'dec',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'feb',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2022' / 'jan',
        Path.home() / 'Downloads' / 'archive (17)' / 'stock_market' / 'data(99-23)' / '2023',
    ]

    target_raw_folder = Path(__file__).resolve().parent.parent / 'datasets' / 'raw_datasets'
    target_raw_folder.mkdir(parents= True, exist_ok = True)

    for source_path in source_paths:
        if source_path.exists():
            for file in source_path.glob('*.csv'):
                target_file_path = target_raw_folder / file.name
                if not target_file_path.exists():
                    shutil.copy(file, target_file_path)
                    print(f"Copied {file.name} to {target_raw_folder}")

                else:
                    print(f"File {file.name} already exists in {target_raw_folder}, skipping copy.")

    print('All files have been copied successfully.')

if __name__ == "__main__":
    import_files()

