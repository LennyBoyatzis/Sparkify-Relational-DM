import pandas as pd
import psycopg2
import ipdb
import sql_queries as sql


class Pipeline():
    def __init__(self, target_db):
        self.db = target_db

    def build(self, config):
        self.config = config
        return self

    def extract(self):
        """Read in data from files"""

        filepaths = self.config['extract']['filepaths']
        num_files = len(filepaths)
        self.data = pd.concat([pd.read_json(f, lines=True) for f in filepaths],
                              ignore_index=True)

        print('Extract: {} files found'.format(num_files))
        return self

    def transform(self):
        """Convert data into the required form"""

        columns = self.config['transform']['columns']
        self.data = self.data[columns]
        return self

    def load(self):
        """Write data to target database"""

        try:
            query = self.config['load']['query']
            self.db.executemany(query, self.data.values.tolist())
        except psycopg2.Error as e:
            print("Pipeline Error", e)

        print('Load: {} records processed'.format(len(self.data.values)))
