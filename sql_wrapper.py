import datetime
import os
import sqlite3 as sql
import typing as t

class FileDBWrapper():
    def __init__(self, db_path: str):
        self._connection = sql.connect(db_path, autocommit=True)
        sql_str = """CREATE TABLE IF NOT EXISTS file_state ( 
filename TEXT NOT NULL PRIMARY KEY,
directory TEXT NOT NULL,
sha384 TEXT NOT NULL,
filesize INTEGER NOT NULL
)"""
        self._connection.execute(sql_str)
        self._connection.commit()

        sql_str = """CREATE TABLE IF NOT EXISTS db_info ( 
key TEXT PRIMARY KEY,
value TEXT
)"""
        self._connection.execute(sql_str)
        self._connection.commit()

    def _write_single_metadatum(self, key: str, value: str) -> None:
        sql_str = """INSERT INTO db_info (key, value) VALUES (?, ?)"""
        self._connection.execute(sql_str, (key, value))
        self._connection.commit()
        return

    def write_start_metadata(self, dir_base: str, start_time: datetime.datetime) -> None:
        self._write_single_metadatum("dir_base", dir_base)
        self._write_single_metadatum("start_time", start_time.strftime("%Y-%m-%d %H:%M:%S"))

    def write_end_metadata(self, end_time: datetime.datetime) -> None:
        self._write_single_metadatum("end_time", end_time.strftime("%Y-%m-%d %H:%M:%S"))

    def does_file_exist(self, filename: str) -> bool:
        result = self._connection.execute("SELECT filename FROM file_state WHERE filename = ?", (filename,))
        self._connection.commit()
        res_data = result.fetchall()
        return len(res_data) == 1

    def record_file(self, filename: str, sha384: str, file_size: int) -> None:
        try:
            directory = os.path.dirname(filename)
            self._connection.execute("INSERT INTO file_state (filename, directory, sha384, filesize) VALUES (?, ?, ?, ?)", (filename, directory, sha384, file_size))
            self._connection.commit()
        except Exception as e:
            print(f"Failed to record file '{filename}': {e}")
            raise e


    def get_duplicate_sha384(self) -> t.List[str]:
        sql_str="SELECT fs.sha384, COUNT(*) as num_duplicates FROM file_state as fs GROUP BY fs.sha384 HAVING num_duplicates > 1"
        result = self._connection.execute(sql_str).fetchall()
        return [v[0] for v in result]

    def get_files_for_sha384hash(self, sha384: str) -> t.List[str]:
        sql_str="SELECT fs.filename FROM file_state as fs WHERE fs.sha384 = ?"
        result = self._connection.execute(sql_str, (sha384, )).fetchall()
        return [v[0] for v in result]

    def close(self) -> None:
        self._connection.close()


