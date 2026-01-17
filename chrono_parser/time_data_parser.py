import pandas as pd
import pytz
from pathlib import Path
import zipfile
import tempfile
from chrono_parser.sqlite_reader import SQliteReader

class TimeDataParser:

    def parse(self, filepath) -> pd.DataFrame:
        """Parse the ATracker backup and return a dataframe of time entries."""
        path_to_sqlite = self._extract_locations_from_atracker(filepath)
        [task_df, task_entries_df] = SQliteReader.read(path_to_sqlite)
        merged_df = self._merge_dfs([task_df, task_entries_df])
        merged_df_with_corrected_timestamps = self._correct_timestamps(merged_df)
        merged_df_with_cet_timestamps = self._convert_timestamps_to_cet(merged_df_with_corrected_timestamps)
        time_data_df = self._calculate_duration(merged_df_with_cet_timestamps)
        return time_data_df

    def _merge_dfs(self, data:list[pd.DataFrame]) -> pd.DataFrame:
        """Join tasks and task entries into a single dataframe."""
        merged_df = data[1].merge(data[0][['ZTASKID', 'ZNAME']], on='ZTASKID', how='left')
        merged_df = merged_df[['ZNAME', 'ZSTARTTIME', 'ZENDTIME']]
        return merged_df
    
    def _correct_timestamps(self, merged_df:pd.DataFrame) -> pd.DataFrame:
        """Convert ATracker epoch seconds into UTC timestamps."""
        offset = 978307150.0
        merged_df['ZSTARTTIME'] = pd.to_datetime(merged_df['ZSTARTTIME'] + offset, unit='s', utc=True)
        merged_df['ZENDTIME'] = pd.to_datetime(merged_df['ZENDTIME'] + offset, unit='s', utc=True)
        return merged_df
    
    def _convert_timestamps_to_cet(self, merged_df:pd.DataFrame) -> pd.DataFrame:
        """Convert UTC timestamps into CET for local analysis."""
        cet = pytz.timezone('CET')
        merged_df['ZSTARTTIME'] = merged_df['ZSTARTTIME'].apply(lambda x: x.astimezone(cet))
        merged_df['ZENDTIME'] = merged_df['ZENDTIME'].apply(lambda x: x.astimezone(cet))
        return merged_df
    
    def _calculate_duration(self, merged_df:pd.DataFrame) -> pd.DataFrame:
        """Add a duration column derived from start and end times."""
        merged_df['ZDURATION'] = merged_df['ZENDTIME'] - merged_df['ZSTARTTIME']
        return merged_df

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
