import sqlite3
from sqlite3 import Error
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

class Db:
    def __init__(self, db_file):
        self.db_file = db_file

    def connect(self):
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
        except Error as e:
            print(e)
        return conn

    def create_table(self, name, columns):
        conn = self.connect()
        sql = 'CREATE TABLE %s (%s);'%(name, ', '.join(columns))
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

    def drop_table(self, name):
        conn = self.connect()
        sql = 'DROP TABLE IF EXISTS %s;'%(name)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()

    def select(self, table, columns, conditions):
        conn = self.connect()
        sql = 'SELECT %s FROM %s WHERE %s;' % (
            ', '.join(columns),
            table,
            ' AND '.join(conditions))
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return rows

    def select_inner_join(self, table, columns, conditions, inner_joins):
        conn = self.connect()
        sql = 'SELECT %s FROM %s %s WHERE %s;' % (
            ', '.join(columns),
            table,
            'INNER JOIN ' + ' INNER JOIN '.join(inner_joins),
            ' AND '.join(conditions))
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        return rows

    def insert(self, table, row, options=''):
        conn = self.connect()
        columns = ', '.join([str(x) for x in row.keys()])
        values = ', '.join([str(x) for x in row.values()])
        sql = 'INSERT OR IGNORE INTO %s (%s) VALUES (%s);' % (table, columns, values)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        return cur.lastrowid

    def reset_tables(self, tables):
        conn = self.connect()
        for table in tables:
            self.drop_table(table)
            self.create_table(table, tables[table])

    def reset_factory_tweets(self):
        conn = self.connect()
        tables = dict()
        tables['tweets'] = ['id BIGINT PRIMARY KEY',
                            'text VARCHAR(300) NOT NULL',
                            'created_at TIMESTAMP NOT NULL',
                            'in_reply_to_status_id BIGINT',
                            'in_reply_to_user_id BIGINT',
                            'retweeted_status_id BIGINT',
                            'quoted_status_id BIGINT',
                            'quote_count INT NOT NULL',
                            'reply_count INT NOT NULL',
                            'retweet_count INT NOT NULL',
                            'favorite_count INT NOT NULL',
                            'user_id BIGINT NOT NULL',
                            'FOREIGN KEY (user_id) REFERENCES users(id)']
        tables['hashtags'] = ['id INTEGER PRIMARY KEY AUTOINCREMENT',
                              'hashtag VARCHAR(100) NOT NULL',
                              'UNIQUE (hashtag)']
        tables['tweets_hashtags'] = ['tweet_id BIGINT NOT NULL',
                                     'hashtag_id INT NOT NULL',
                                     'PRIMARY KEY (tweet_id, hashtag_id)',
                                     'FOREIGN KEY (tweet_id) REFERENCES tweets(id)',
                                     'FOREIGN KEY (hashtag_id) REFERENCES hashtags(id)']
        tables['symbols'] = ['id INTEGER PRIMARY KEY AUTOINCREMENT',
                             'symbol VARCHAR(100) NOT NULL',
                             'UNIQUE (symbol)']
        tables['tweets_symbols'] = ['tweet_id BIGINT NOT NULL',
                                    'symbol_id INT NOT NULL',
                                    'PRIMARY KEY (tweet_id, symbol_id)',
                                    'FOREIGN KEY (tweet_id) REFERENCES tweets(id)',
                                    'FOREIGN KEY (symbol_id) REFERENCES symbols(id)']
        tables['users'] = ['id BIGINT PRIMARY KEY',
                           'name VARCHAR(100) NOT NULL',
                           'screen_name VARCHAR(100) NOT NULL',
                           'verified BOOLEAN NOT NULL',
                           'followers_count INT NOT NULL',
                           'friends_count INT NOT NULL',
                           'listed_count INT NOT NULL',
                           'favourites_count INT NOT NULL',
                           'statuses_count INT NOT NULL',
                           'created_at TIMESTAMP NOT NULL']

        self.reset_tables(tables)
