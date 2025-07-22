📈 DSE Historical Stock Data Pipeline (1999–2023)
This project is a complete Data Engineering pipeline focused on the Dhaka Stock Exchange (DSE). It covers everything from data collection to loading into a Snowflake Data Warehouse using modern data engineering tools like Airflow, Pandas, and Snowflake Connector.

✅ Project Highlights

* Source: Historical stock data from DSE (1999–2023)
* Volume: ~1.3 million rows
* Tech Stack: Python, Pandas, Apache Airflow, Snowflake (will be integrating PySpark gradually)
* Goal: Building a scalable pipeline to automate the end-to-end stock data processing

🚀 Features & Workflow
1. 📦 Data Collection
* Collected raw historical stock data from DSE (1999–2023)
* Stored as multiple .csv files in the project

2. 🔍 Data Extraction & Transformation
* Extracted raw CSV files using Python
* Cleaned and transformed using Pandas
* Standardized column names
* Removed rows with nulls or duplicates
* Sanity checks of all the data of respesctive columns 
* Parsed and formatted dates
* Stored the clean data in /datasets/cleaned_dataset

3. ⛓ Automated with Apache Airflow
Built DAGs to automate:
* Extraction
* Cleaning
* Loading into the clean folder

4. 🧊 Data Warehouse Integration with Snowflake
* Loaded the clean, processed data into Snowflake
* Used chunked insertions for large volume (~50,000 rows per batch)
* Ensured schema consistency and data type handling
