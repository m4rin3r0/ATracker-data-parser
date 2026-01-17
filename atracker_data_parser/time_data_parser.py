import pandas as pd
import pytz
from atracker_data_parser.sqlite_reader import SQliteReader

class TimeDataParser:

    def parse(self, filepath) -> pd.DataFrame:
        """Parse the ATracker backup and return a dataframe of time entries."""
        fabian_reader = SQliteReader(filepath)
        task_df = fabian_reader.read_tasks()
        task_entries_df = fabian_reader.read_task_entries()
        tag_df = fabian_reader.read_tags()
        task_tag_df = fabian_reader.read_task_tags()
        fabian_reader.close()
        merged_df = self._merge_dfs(task_df, task_tag_df, tag_df, task_entries_df)
        df_with_corrected_timestamps = self._correct_timestamps(merged_df)
        df_with_cet_timestamps = self._convert_timestamps_to_cet(df_with_corrected_timestamps)
        time_data_df = self._calculate_duration(df_with_cet_timestamps)
        return time_data_df

    def _merge_dfs(self, task_df:pd.DataFrame, task_tag_df:pd.DataFrame, tag_df:pd.DataFrame, task_entries_df:pd.DataFrame) -> pd.DataFrame:
        """Join tasks, task entries and tags into a single dataframe."""
        merged_df = task_tag_df.merge(tag_df, on='ZTAGID', how='left')
        merged_df = task_entries_df.merge(merged_df, on='ZTASKID', how='left')
        merged_df = merged_df.rename(columns={'ZNAME': 'ZTAG'})
        merged_df = merged_df[['ZTASKID', 'ZTAG', 'ZSTARTTIME', 'ZENDTIME']]
        merged_df = merged_df.merge(task_df, on='ZTASKID', how='left')
        merged_df = merged_df[['ZNAME', 'ZTAG', 'ZSTARTTIME', 'ZENDTIME']]
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

    
