"""
This module used for create SCC Monitor record history.\n
User can create SQLite database or Insert/Update/Delete table
"""
import sys
import traceback
import sqlite3
import logging
from pathlib import Path, PurePath


class SQLiteDB():
    def __init__(self, db_dir, db_name, component) -> None:
        self._db_file_path = self.get_db_dir(db_dir, db_name)
        self.create_table(comp=component)

    def get_db_dir(self, db_dir, db_name):
        try:
            Path(db_dir).mkdir(parents=True, exist_ok=True)
            return PurePath(db_dir).joinpath(db_name)
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')

    def create_connection(self) -> sqlite3.Connection:
        try:
            _conn = None
            _conn = sqlite3.connect(self._db_file_path)
        except sqlite3.Error:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
        finally:
            return _conn

    def create_table(self, comp: str):
        if Path(self._db_file_path).exists() is True:
            return
        logging.info(f'Create Table in {self._db_file_path}')
        server_sql = {}
        storage_sql = {}
        server_sql['Fans'] = """
        CREATE TABLE IF NOT EXISTS Fans (
            sampling_time TEXT NOT NULL,
            fan_bay TEXT NOT NULL,
            status TEXT NOT NULL,
            speed_percent TEXT NOT NULL,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        server_sql['Temp'] = """
        CREATE TABLE IF NOT EXISTS Temperature (
            sampling_time TEXT NOT NULL,
            name TEXT NOT NULL,
            location_group TEXT NOT NULL,
            status TEXT NOT NULL,
            reading INT NOT NULL,
            caution INT NOT NULL,
            percent_over FLOAT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        server_sql['PowerSupplies'] = """
        CREATE TABLE IF NOT EXISTS PowerSupplies (
            sampling_time TEXT NOT NULL,
            name TEXT,
            present TEXT,
            status   TEXT,
            reading TEXT,
            capacity TEXT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        server_sql['Memories'] = """
        CREATE TABLE IF NOT EXISTS Memories (
            sampling_time TEXT NOT NULL,
            slot TEXT,
            cpu TEXT,
            status TEXT,
            type   TEXT,
            size TEXT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        server_sql['Network'] = """
        CREATE TABLE IF NOT EXISTS Network (
            sampling_time TEXT NOT NULL,
            location TEXT,
            port TEXT,
            status TEXT,
            description TEXT,
            ip_address TEXT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        server_sql['Control'] = """
        CREATE TABLE IF NOT EXISTS Controller (
            sampling_time TEXT NOT NULL,
            name TEXT,
            status TEXT,
            controller_status TEXT,
            cache_module_status TEXT,
            cache_module_memory TEXT,
            model TEXT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        server_sql['Disk'] = """
        CREATE TABLE IF NOT EXISTS Disk (
            sampling_time TEXT NOT NULL,
            name TEXT,
            status TEXT,
            capacity TEXT,
            fw_version TEXT,
            serial_number TEXT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        server_sql['VDisk'] = """
        CREATE TABLE IF NOT EXISTS VDisk (
            sampling_time TEXT NOT NULL,
            name TEXT,
            status TEXT,
            capacity TEXT,
            fault_tolerance TEXT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        storage_sql['Disk'] = """
        CREATE TABLE IF NOT EXISTS Disk (
            sampling_time TEXT NOT NULL,
            location TEXT NOT NULL,
            status   TEXT NOT NULL,
            health TEXT NOT NULL,
            health_reason TEXT,
            how_used TEXT NOT NULL,
            current_job TEXT NOT NULL,
            data_transferred TEXT NOT NULL,
            avg_rsp_time INTEGER NOT NULL,
            iops TEXT,
            reads_count TEXT,
            writes_count TEXT,
            data_read TEXT,
            data_written TEXT,
            reset_time TEXT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        storage_sql['VDisk'] = """
        CREATE TABLE IF NOT EXISTS VDisk (
            sampling_time TEXT NOT NULL,
            name TEXT NOT NULL,
            owner TEXT NOT NULL,
            status   TEXT NOT NULL,
            health TEXT NOT NULL,
            health_reason TEXT,
            raid_type TEXT NOT NULL,
            current_job TEXT NOT NULL,
            disk_count TEXT NOT NULL,
            create_date TEXT NOT NULL,
            reads_count TEXT NOT NULL,
            writes_count TEXT NOT NULL,
            data_transferred TEXT NOT NULL,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        storage_sql['Controller'] = """
        CREATE TABLE IF NOT EXISTS Controller (
            sampling_time TEXT,
            name TEXT NOT NULL,
            ip_address TEXT,
            status   TEXT,
            health TEXT,
            health_reason TEXT,
            failed_over TEXT,
            failed_over_reason TEXT,
            mfg_date TEXT,
            position TEXT,
            redundancy_mode INTEGER,
            redundancy_status INTEGER,
            power_on_time TEXT,
            reads_count TEXT,
            writes_count TEXT,
            data_read TEXT,
            data_written TEXT,
            reset_time TEXT,
            total_power_on_hours TEXT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        storage_sql['PowerSupplies'] = """
        CREATE TABLE IF NOT EXISTS PowerSupplies (
            sampling_time TEXT NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL,
            health TEXT NOT NULL,
            health_reason TEXT,
            dc12v TEXT NOT NULL,
            dc5v TEXT NOT NULL,
            dc33v TEXT NOT NULL,
            dc12i TEXT NOT NULL,
            dc5i TEXT NOT NULL,
            psu_temp TEXT NOT NULL,
            mfg_date TEXT NOT NULL,
            fan_name   TEXT,
            fan_status TEXT,
            fan_speed_RPM TEXT,
            fan_health TEXT,
            fan_health_reason TEXT,
            is_alarmed INTEGER NOT NULL CHECK (is_alarmed IN (0, 1)) DEFAULT 0
        )
        """
        storage_sql['SessionKey'] = """
        CREATE TABLE IF NOT EXISTS SessionKey (
            ip TEXT NOT NULL,
            proto TEXT NOT NULL,
            expired TEXT NOT NULL,
            skey TEXT NOT NULL DEFAULT 0,
            PRIMARY KEY (ip, proto)
        )
        """
        try:
            conn = self.create_connection()
            cursor = conn.cursor()
            create_sql = server_sql if comp in ('L2SCC01', 'L2SCC02') else storage_sql
            for table_name, sql_text in create_sql.items():
                cursor.execute(sql_text.upper())
        except sqlite3.OperationalError as e:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
        finally:
            conn.commit()
            cursor.close()
            conn.close()

    def execute_sql(self, query: str, fetch_all=False):
        conn = self.create_connection()
        data = "No data"
        if conn:
            cursor = conn.cursor()
            try:
                if not fetch_all:
                    data = cursor.execute(query).fetchone()
                else:
                    data = cursor.execute(query).fetchall()
                logging.info(f'Executed "{query}"')
            except sqlite3.OperationalError as e:
                exc_type, exc_val, exc_tb = sys.exc_info()
                throw_exc = traceback.extract_tb(exc_tb)[0]
                logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val} [{query}]')
            conn.commit()
            cursor.close()
            conn.close()
            return data
        else:
            logging.error(f'Connect Failed {self._db_file_path}')
