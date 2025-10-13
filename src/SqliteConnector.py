import sqlite3   



class SqliteConnector:

    def __init__(self, conn_string: str) -> None:
        self.conn_string: str = conn_string


    def execute_query(self, sql_query: str, do_logging: bool = False) -> list | None:
        try:
            with sqlite3.connect(self.conn_string) as conn:
                cursor = conn.cursor() 
                cursor.execute(sql_query)
                result = cursor.fetchall()
                return result
            
        except sqlite3.OperationalError:
            do_logging and print("[Warning] Sql Error")
            return None

    def test(self, do_logging: bool) -> bool:
        sqliteConnection = None
        try:
            # Connect to SQLite Database and create a cursor
            sqliteConnection = sqlite3.connect(self.conn_string)
            cursor = sqliteConnection.cursor()

            do_logging and print('[LOG] DB conn init')

            # Execute a query to get the SQLite version
            query = 'SELECT sqlite_version();'
            cursor.execute(query)

            # Fetch and print the result
            result = cursor.fetchall()
            do_logging and print('[LOG] SQLite Version is {}'.format(result[0][0]))

            # Close the cursor after use
            cursor.close()

        except sqlite3.Error as error:
            print('Error occurred -', error)
            return False

        finally:
            # Ensure the database connection is closed
            if sqliteConnection:
                sqliteConnection.close()
                do_logging and print('[LOG] SQLite Connection closed')

        return True

    def has_table(self, tablename: str) -> bool:
        table_list = self.execute_query(f"""
            SELECT name 
            FROM sqlite_master 
            WHERE type='table'
            AND name='{tablename}';"""
        )
        if table_list is None:
            return False

        return len(table_list) != 0
    
    def tables_names(self) -> list[str]:
        name_list = self.execute_query("""
            SELECT name 
            FROM sqlite_master 
            WHERE type='table';"""
        )

        if name_list is None:
            return []
        
        return list(map(lambda x: x[0], name_list))
    

if __name__ == "__main__":
    db_filepath = "./db/Chinook.db"
    db = SqliteConnector(db_filepath)

    assert db.execute_query("this is not sql") is None
    print(db.execute_query("SELECT * FROM Album LIMIT 3;"))