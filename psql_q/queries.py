import psycopg2

from psql_q.config import get_config


class DataBaseConnection:
    """Connect to postgres"""
    def __init__(self):
        try:
            self.connection = psycopg2.connect(**get_config())
            self.cursor = self.connection.cursor()
            print('Connecting to the PostgreSQL database...')
            print('PostgreSQL database version:')
            self.cursor.execute('SELECT version()')
            db_version = self.cursor.fetchone()
            print(db_version)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)


class Queries(DataBaseConnection):
    def create_table(self, command):
        self.cursor = self.connection.cursor()
        try:
            self.cursor.execute(command)
            print(f'Table is created!')
        except psycopg2.ProgrammingError as error:
            print(error)
        self.cursor.close()
        self.connection.commit()

    def drop_table(self, table_name):
        self.cursor = self.connection.cursor()
        sql = f'DROP TABLE {table_name}'
        try:
            self.cursor.execute(sql)
            print(f'Table "{table_name}" is drop!')
        except psycopg2.ProgrammingError as error:
            print(error)
        self.cursor.close()
        self.connection.commit()

    def select_table(self, command, limit=5):
        self.cursor = self.connection.cursor()
        self.cursor.execute(command)
        rows = self.cursor.fetchall()
        for row in rows[:limit]:
            print(row)
        self.connection.commit()
        self.cursor.close()
        return rows

    def insert_new_record(self, command):
        self.cursor = self.connection.cursor()
        try:
            self.cursor.execute(command)
            print('Insert new record!')
        except psycopg2.IntegrityError as error:
            print(error)
        self.connection.commit()
        self.cursor.close()

    def update_record(self, command):
        self.cursor = self.connection.cursor()
        try:
            self.cursor.execute(command)
            print('Record updated!')
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as error:
            print('Record not updated!')
            print(error)
        self.connection.commit()
        self.cursor.close()

    def delete_row(self, command):
        """Delete row"""
        self.cursor = self.connection.cursor()
        try:
            self.cursor.execute(command)
            print('Row delete!')
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as error:
            print('Row not deleted!')
            print(error)
        self.connection.commit()
        self.cursor.close()

    def add_new_column(self, command):
        """Add new column"""
        self.cursor = self.connection.cursor()
        try:
            self.cursor.execute(command)
            print('ADD new column!')
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as error:
            print('New column not added!')
            print(error)
        self.connection.commit()
        self.cursor.close()

    def delete_column(self, command):
        """Delete column"""
        self.cursor = self.connection.cursor()
        try:
            self.cursor.execute(command)
            print('Delete column!')
        except (psycopg2.ProgrammingError, psycopg2.IntegrityError) as error:
            print('Column not deleted!')
            print(error)
        self.connection.commit()
        self.cursor.close()
