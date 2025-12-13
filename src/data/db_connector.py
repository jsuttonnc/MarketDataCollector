import os
import platform
import psycopg2
from typing import Any
from psycopg2.extras import DictCursor, execute_values


class DatabaseConnector:
    """Class responsible for PostgreSQL database connections"""

    def __init__(self):
        # get the current OS
        current_os = platform.system().upper()

        # Database configuration
        self.db_params = {
            'dbname': os.getenv('DB_NAME', 'postgres'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_' + current_os + '_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432')
        }
        self.connection = None
        self.cursor = None


    def connect(self) -> None:
        """Establish connection to the PostgreSQL database"""
        try:
            self.connection = psycopg2.connect(**self.db_params)
            self.cursor = self.connection.cursor(cursor_factory=DictCursor)
            print("Successfully connected to the database")
        except psycopg2.Error as e:
            print(f"Error connecting to the database: {e}")
            raise


    def disconnect(self) -> None:
        """Close the database connection"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
                print("Database connection closed")
        except psycopg2.Error as e:
            print(f"Error disconnecting from the database: {e}")
            raise


    def execute_query(self, query: str, params: tuple = None) -> Any:
        """
        Execute a SQL query and return the results

        Args:
            query: SQL query string
            params: Optional tuple of parameters for the query

        Returns:
            Query results
        """
        try:
            self.cursor.execute(query, params)
            if query.strip().upper().startswith(('SELECT', 'SHOW')):
                return self.cursor.fetchall()
            self.connection.commit()
            return None
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Error executing query: {e}")
            raise

    def execute_many(self, query: str, params_list: list[tuple], page_size: int = 1000) -> None:
        """
        Bulk execute for INSERT statements using psycopg2.extras.execute_values.

        Expects `query` to be an INSERT with a single VALUES %s placeholder, e.g.:
            INSERT INTO table (a,b,c) VALUES %s
        """
        if not params_list:
            return

        try:
            execute_values(self.cursor, query, params_list, page_size=page_size)
            self.connection.commit()
        except psycopg2.Error:
            self.connection.rollback()
            raise


    def __enter__(self):
        """Context manager entry point"""
        self.connect()
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point"""
        self.disconnect()