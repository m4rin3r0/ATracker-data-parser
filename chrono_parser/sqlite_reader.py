import sqlite3
import pandas as pd
from pathlib import Path

class SQliteReader:

    @staticmethod
    def read(filepath: Path|str) -> list[pd.DataFrame]:
        """Reads the ZTASK and ZTASKENTRY tables from the given SQLite file."""    
        conn = sqlite3.connect(filepath)
        task_df = pd.read_sql_query("SELECT * FROM ZTASK", conn)
        task_entries_df = pd.read_sql_query("SELECT * FROM ZTASKENTRY", conn)
        conn.close()
        return [task_df, task_entries_df]

    
