import sqlite3
import tempfile
import zipfile
import pandas as pd
from pathlib import Path

class SQliteReader:

    def __init__(self, backup_path: Path|str):
        """Initializes the SQLite reader by extracting the Locations.sqlite file"""
        filepath = self._extract_locations_from_atracker(backup_path)
        self.conn = sqlite3.connect(filepath)

    def read_tasks(self) -> pd.DataFrame:
        """Reads the ZTASK table from the SQLite file."""
        task_df = pd.read_sql_query("SELECT * FROM ZTASK", self.conn)
        return task_df
    
    def read_task_entries(self) -> pd.DataFrame:
        """Reads the ZTASKENTRY table from the SQLite file."""
        task_entries_df = pd.read_sql_query("SELECT * FROM ZTASKENTRY", self.conn)
        return task_entries_df
    
    def read_tags(self) -> pd.DataFrame:
        """Reads the ZTAG table from the SQLite file."""
        tag_df = pd.read_sql_query("SELECT * FROM ZTAG", self.conn)
        return tag_df
    
    def read_task_tags(self) -> pd.DataFrame:
        """Reads the ZTASKTAG table from the SQLite file."""
        task_tag_df = pd.read_sql_query("SELECT * FROM ZTASKTAG", self.conn)
        return task_tag_df
    
    def close(self):
        """Closes the database connection."""
        self.conn.close()

    def _extract_locations_from_atracker(self, atracker_path) -> Path:
        """Extracts the first `Locations.sqlite` found inside a backup.ATracker (zip) archive.
        Returns a Path to the extracted sqlite file. The file is extracted into a
        temporary directory (which is not automatically deleted) so callers can
        open it with SQLite.
        """
        atracker_path = Path(atracker_path)
        if not atracker_path.exists():
            raise FileNotFoundError(f"Archive not found: {atracker_path}")
        with zipfile.ZipFile(atracker_path, 'r') as z:
            candidates = [n for n in z.namelist() if Path(n).name == 'Locations.sqlite']
            if not candidates:
                raise ValueError('Locations.sqlite not found in archive')
            member = candidates[0]
            outdir = Path(tempfile.mkdtemp(prefix='atracker_'))
            z.extract(member, path=outdir)
            extracted_path = (outdir / Path(member)).resolve()
            return extracted_path


    
