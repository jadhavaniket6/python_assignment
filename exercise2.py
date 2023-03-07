import pandas as pd
from sqlalchemy import create_engine


class CSVToDatabase:
    def __init__(self, csv_paths, db_host, db_schema):
        self.csv_paths = csv_paths
        self.db_host = db_host
        self.db_schema = db_schema

    def read_csv(self, csv_path):
        return pd.read_csv(csv_path)

    def write_to_database(self, df, table_name):
        engine = create_engine(f'postgresql://{self.db_host}/{self.db_schema}')
        df.to_sql(table_name, engine, if_exists='replace')

    def process_csv_files(self, table_names):
        for i, csv_path in enumerate(self.csv_paths):
            df = self.read_csv(csv_path)
            table_name = table_names[i]
            self.write_to_database(df, table_name)


'''
INPUTS:
csv_paths = ['path/to/file1.csv', 'path/to/file2.csv', 'path/to/file3.csv']
table_names = ['table1', 'table2', 'table3']
db_host = 'localhost:5432'
db_schema = 'my_database'

csv_to_db = CSVToDatabase(csv_paths, db_host, db_schema)
csv_to_db.process_csv_files(table_names)
'''
