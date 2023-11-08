
import psycopg2 as pg


class DatabaseConnection:

    def __init__(self, psycopg_connection_string: str, name: str):
        self.connection_string = psycopg_connection_string
        self.name = name

    def __str__(self):
        return f"Database Connection: {self.name}"

    def establish_connection(self):
        try:
            connection = pg.connect(self.connection_string)
            # https://www.psycopg.org/psycopg3/docs/basic/transactions.html#transactions
            connection.autocommit = True
        except ConnectionError:
            raise ConnectionError('Could not connect to database server')
        cursor = connection.cursor()
        return connection, cursor
