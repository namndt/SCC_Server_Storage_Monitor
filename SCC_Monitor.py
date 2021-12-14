from datetime import datetime, timedelta
from time import sleep, time
from typing import List
from pconst import const as _
import servicemanager
import requests
import logging
from logging import config
from pathlib import Path, PurePath
from configparser import ConfigParser
import sys
import traceback
from threading import Thread, Event
from hashlib import md5
from xml.etree import ElementTree as ET
import hpilo
from SCC_Monitor_Service import ServiceBase
from SCC_Database import SQLiteDB


def ConfigLogger():
    LOG_FILE_NAME = 'SCC_Monitor_Log.txt'
    application_path = Path(sys.executable).parent

    LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': True,
        'formatters': {
            'standard': {
                'format': '%(asctime)s [%(levelname)5s] %(thread)5d %(filename)-15s|%(funcName)18s() : %(message)s'
            },
        },
        'handlers': {
            'default_handler': {
                'class': 'logging.handlers.RotatingFileHandler',
                'level': 'DEBUG',
                'formatter': 'standard',
                'filename': PurePath(application_path).joinpath(LOG_FILE_NAME),
                'encoding': 'utf8',
                'mode': 'a',
                'maxBytes': 10485760,
                'backupCount': 10,
            }
        },
        'loggers': {
            '': {
                'handlers': ['default_handler'],
                'level': 'NOTSET',
                'propagate': False,
            }
        }
    }
    config.dictConfig(LOGGING_CONFIG)


class INIConfiguration():
    INI_FILE_NAME = 'SCC_Monitor_Settings.ini'
    application_path = Path(sys.executable).parent

    def __init__(self):
        self.__config = ConfigParser()
        self.__ini_full_path = PurePath(self.application_path).joinpath(self.INI_FILE_NAME)
        if Path(self.__ini_full_path).exists() is False:
            self.create_ini_file()

    def read(self, section, key):
        self.__config.read(self.__ini_full_path)
        value = self.__config.get(section=section, option=key)
        return value

    def create_ini_file(self):
        logging.info(f'"{self.__ini_full_path}" is not exist. Creating new INI file...')
        self.__config['LINE_NOTIFY'] = {
            'line_token': 'DspCsZ5L9nvCRhJdUDhEwx4QqiDCa1iabJqbrcA6VxB',
            'line_api_url': 'http://10.199.15.109:8080/api/line/notify.php'
        }
        self.__config['L2SCC01'] = {
            'ilo_host': '192.168.152.44',
            'ilo_username': 'ilouser',
            'ilo_password': '12345678',
            'over_temperature_percent': 80,
            'history_database_location': Path.cwd(),
            'monitoring_history_file_name': 'L2SCC01_Monitor_History.db',
        }
        self.__config['L2SCC02'] = {
            'ilo_host': '192.168.152.45',
            'ilo_username': 'ilouser',
            'ilo_password': '12345678',
            'over_temperature_percent': 80,
            'history_database_location': Path.cwd(),
            'monitoring_history_file_name': 'L2SCC02_Monitor_History.db',
        }
        self.__config['MSA_STORAGE'] = {
            'msa_host': '10.0.0.3',
            'msa_username': 'manage',
            'msa_password': '!manage',
            'history_database_location': Path.cwd(),
            'monitoring_history_file_name': 'Storage_Monitor_History.db',
        }
        self.__config['SETTING'] = {
            'monitoring_interval_hour': 1,
        }

        Path(self.__ini_full_path).touch(mode=0o777, exist_ok=True)
        with Path(self.__ini_full_path).open(mode='w') as configFile:
            self.__config.write(configFile)


class LineNotifyThread(Thread):
    def __init__(self) -> None:
        super().__init__()
        self.__event = Event()
        self.__msg_to_send = 'No Data'
        self.apply_config()

    def apply_config(self):
        try:
            self.LINE_NOTIFY_API_URL = iniconfig.read('LINE_NOTIFY', 'line_api_url')
            self.LINE_TOKEN = iniconfig.read('LINE_NOTIFY', 'line_token')
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')

    def send_notify(self, message: str = None):
        __inMsg = self.__msg_to_send if message is None else message
        data = {
            'token': self.LINE_TOKEN,
            'message': __inMsg,
        }
        try:
            response = requests.post(url=self.LINE_NOTIFY_API_URL, data=data)
            logging.info(f'POST "{self.LINE_NOTIFY_API_URL}" {response.status_code} "{__inMsg.replace(chr(10), "; ")}"')
        except requests.exceptions.RequestException as e:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')

    def humman_format(self, equipment, state, start_time, reason=None, comp=None, isHandled=False):
        message = 'No data'
        if isHandled is False:
            message = (
                f'{comp}設備異常\n'
                f'⁕設備名稱: {equipment}\n'
                f'⁕目前狀態: {state}\n'
                f'⁕原因: {reason}\n'
                f'⁕發生時間: {start_time}'
            )
        else:
            message = (
                f'{comp}設備已復原\n'
                f'⁕設備名稱: {equipment}\n'
                f'⁕目前狀態: {state}\n'
                f'⁕復原時間: {start_time}'
            )
        self.__msg_to_send = message

    def repeated_send(self):
        while True:  # Keep thread never die
            present = datetime.now().replace(second=0, microsecond=0)
            repeated_minutes = [0, 5, 10, 15, 20]
            executed_time_ls = [present + timedelta(minutes=sum(repeated_minutes[:idx + 1])) for idx, val in enumerate(repeated_minutes)]
            idx = 0
            while idx != len(executed_time_ls):
                current_time = datetime.now().__format__('%Y-%m-%d %H:%M')
                if not self.__event.is_set():
                    if current_time == executed_time_ls[idx].__format__('%Y-%m-%d %H:%M'):
                        self.send_notify(self.__msg_to_send)
                        idx += 1
                    sleep(10)
                else:
                    break
            sleep(10)

    def repaired_completed(self):
        if self.__event.wait():
            self.send_notify()
            logging.debug(f'Event set to {self.__event.is_set()}')
            logging.info(f'Thread [{self.name}] Id [{self.ident}] The problem has been solved.')
        self.__event.clear()

    def trouble_occurs(self):
        self.__event.set()
        logging.debug(f'Event set to {self.__event.is_set()}')

    def run(self):
        self.repeated_send()


class HPiLO_Monitor():
    def __init__(self, server: str) -> None:
        self.__th_fan = LineNotifyThread()
        self.__th_temp_error = LineNotifyThread()
        self.__th_temp_high = LineNotifyThread()
        self.__th_power = LineNotifyThread()
        self.__th_memory = LineNotifyThread()
        self.__th_disk = LineNotifyThread()
        self.__th_vdisk = LineNotifyThread()
        self.__th_network = LineNotifyThread()
        self.__th_controller = LineNotifyThread()
        self.SERVER = server
        self.apply_config(server)
        self.dbapi = SQLiteDB(db_dir=self.DB_DIR, db_name=self.DB_NAME, component=self.SERVER)

    def apply_config(self, server: str):
        try:
            if server == 'L2SCC01':
                self.ILO_HOST = iniconfig.read('L2SCC01', 'ilo_host')
                self.ILO_LOGIN = iniconfig.read('L2SCC01', 'ilo_username')
                self.ILO_PASSWORD = iniconfig.read('L2SCC01', 'ilo_password')
                self.OVER_TEMP_PERCENT = float(iniconfig.read('L2SCC01', 'over_temperature_percent'))
                self.DB_DIR = iniconfig.read('L2SCC01', 'history_database_location')
                self.DB_NAME = iniconfig.read('L2SCC01', 'monitoring_history_file_name')
            elif server == 'L2SCC02':
                self.ILO_HOST = iniconfig.read('L2SCC02', 'ilo_host')
                self.ILO_LOGIN = iniconfig.read('L2SCC02', 'ilo_username')
                self.ILO_PASSWORD = iniconfig.read('L2SCC02', 'ilo_password')
                self.OVER_TEMP_PERCENT = float(iniconfig.read('L2SCC02', 'over_temperature_percent'))
                self.DB_DIR = iniconfig.read('L2SCC02', 'history_database_location')
                self.DB_NAME = iniconfig.read('L2SCC02', 'monitoring_history_file_name')

            logging.debug('=' * 80)
            logging.debug(f'ILO_SERVER [{self.SERVER}] HOST [{self.ILO_HOST}] AUTHENTICATION [{self.ILO_LOGIN}@{self.ILO_PASSWORD}]')
            logging.debug(f'ILO_SERVER OVER_TEMP_PERCENT set is {self.OVER_TEMP_PERCENT}%')
            logging.debug('=' * 80)
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')

    def get_server_health(self):
        server_health_dict = {}
        try:
            logging.info(f'Connecting to {self.SERVER} iLO Server management, host {self.ILO_HOST}.')
            ilo = hpilo.Ilo(hostname=self.ILO_HOST, login=self.ILO_LOGIN, password=self.ILO_PASSWORD)
            server_health_dict = ilo.get_embedded_health()
        except hpilo.IloCommunicationError:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_network.send_notify(f'[SCC Monitor]連不到iLO地址{self.ILO_HOST}，請查看IP地址、帳號或密碼。')
            return
        health_summary = server_health_dict['health_at_a_glance']

        self.fan_EWS(server_health_dict['fans'])
        self.temperature_EWS(server_health_dict['temperature'])
        self.power_EWS(server_health_dict['power_supplies'], server_health_dict['power_supply_summary'])
        self.memory_EWS(server_health_dict['memory'])
        self.network_EWS(server_health_dict['nic_information'])
        self.storage_EWS(server_health_dict['storage'])

    def describe(self, status):
        description = {
            'Redundant': 'There is a backup component for the device or subsystem.',
            'OK': 'The device or subsystem is working correctly.',
            'Not Redundant': 'There is no backup component for the device or subsystem.',
            'Not Available': 'The component is not available or not installed.',
            'Degraded': 'The device or subsystem is operating at a reduced capacity.',
            'Failed Redundant': 'The device or subsystem is in a nonoperational state.',
            'Failed': 'One or more components of the device or subsystem are nonoperational.',
            'Link Down': 'The network link is down.',
            'Not Installed': 'The subsystem or device is not installed.',
            'No Supporting CPU': 'The cpu that supports the device slot is not installed.',
        }
        value = 'No data'
        try:
            value = description[status]
        except KeyError:
            value = 'Key not found'
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
        return value

    def fan_EWS(self, fans_data: dict):
        '''Early Warning System for fans subsystem unusual'''
        try:
            sampling_time = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
            fans_err = {}
            for name, info in fans_data.items():
                status = info['status']
                speed = info['speed'][0]  # (speed, unit) get speed value with index 0
                if status not in ('OK', 'Redundant', 'Not Installed'):
                    fans_err[name] = status
                    self.dbapi.execute_sql(f"INSERT INTO FANS (SAMPLING_TIME, FAN_BAY, STATUS, SPEED_PERCENT, IS_ALARMED) VALUES('{sampling_time}', '{name}', '{status}', '{speed}', 1)")
                else:
                    self.dbapi.execute_sql(f"INSERT INTO FANS (SAMPLING_TIME, FAN_BAY, STATUS, SPEED_PERCENT, IS_ALARMED) VALUES('{sampling_time}', '{name}', '{status}', '{speed}', 0)")
            if fans_err.__len__() != 0:  # There is/are fan(s) failed
                logging.error(f'There are {fans_err.__len__()} errors in Server Cooling Fans')
                logging.error(f'Fans trouble detail {fans_err}')
                m_name = ', '.join([name for name in fans_err.keys()])
                m_status = ', '.join([status for status in fans_err.values()])
                self.__th_fan.humman_format(f'冷風扇{m_name}', m_status, sampling_time, self.describe(m_status.split(',')[0]), self.SERVER)
                self.__th_fan.name = 'Fans_EWS'
                if self.__th_fan.is_alive() is False:
                    self.__th_fan.start()
                    logging.debug(f'Continuous LINE Notify is started as [{self.__th_fan.name}] ID [{self.__th_fan.ident}]')
                else:
                    self.__th_fan.trouble_occurs()
            else:
                if self.__th_fan.is_alive():
                    self.__th_fan.humman_format('冷風扇', 'OK', sampling_time, comp=self.SERVER, isHandled=True)
                    self.__th_fan.repaired_completed()
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_fan.send_notify(f'[{self.SERVER}_Monitor]冷風扇監控失敗，請查看Log檔。')
            return

    def temperature_EWS(self, temp_data: dict):
        '''Early Warning System for temperature subsystem unusual'''
        sampling_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_sensors = {}
        high_temp = {}
        isErrorSensor = False
        isHighTemp = False
        try:
            # Grouping sensors that have same location
            installed_sensors = {sensor: info for sensor, info in temp_data.items() if info['status'] != 'Not Installed'}
            group = {info['location']: '' for info in installed_sensors.values()}
            for type in group.keys():
                group[type] = [sensor for sensor, info in installed_sensors.items() if info['location'] == type]
            # Fill missing caution threshold value with mean
            for name, info in installed_sensors.items():
                subgroup = info['location']
                status = info['status']
                i_reading = 0
                i_caution = 0
                percent = 0.0
                count = 0
                if info['caution'] == 'N/A':
                    sum_of_caution = 0
                    for sensor in group[subgroup]:
                        caution = installed_sensors[sensor]['caution']
                        if caution != 'N/A':
                            count += 1
                            sum_of_caution += int(caution[0])
                    try:
                        mean = int(sum_of_caution / count)
                        info['caution'] = (mean, 'Celsius')
                    except Exception:  # ZeroDivisionError
                        exc_type, exc_val, exc_tb = sys.exc_info()
                        throw_exc = traceback.extract_tb(exc_tb)[0]
                        logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
                        continue
                # Check if sensor failure
                if status not in ('OK', 'Redundant'):
                    isErrorSensor = True
                    error_sensors[name] = status
                else:
                    isErrorSensor = False
                # Check if over temperature. Next if failure sensor lead to cannot read temperature value
                try:
                    i_reading = info['currentreading'][0]
                    i_caution = info['caution'][0]
                    percent = (i_reading / i_caution) * 100
                    if percent >= self.OVER_TEMP_PERCENT:
                        isHighTemp = True
                        high_temp[f'{name}({subgroup})'] = i_reading
                    else:
                        isHighTemp = False
                except Exception:
                    exc_type, exc_val, exc_tb = sys.exc_info()
                    throw_exc = traceback.extract_tb(exc_tb)[0]
                    logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
                    i_reading = 0
                    i_caution = 0
                    continue
                if isErrorSensor or isHighTemp:
                    self.dbapi.execute_sql(
                        "INSERT INTO TEMPERATURE (SAMPLING_TIME, NAME, LOCATION_GROUP, STATUS, READING, CAUTION, PERCENT_OVER, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{name}','{subgroup}','{status}','{i_reading}','{i_caution}','{percent:.2f}',1)"
                    )
                else:
                    self.dbapi.execute_sql(
                        "INSERT INTO TEMPERATURE (SAMPLING_TIME, NAME, LOCATION_GROUP, STATUS, READING, CAUTION, PERCENT_OVER, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{name}','{subgroup}','{status}','{i_reading}','{i_caution}','{percent:.2f}',0)"
                    )
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_temp_error.send_notify('[SCC_Monitor]溫度感應器監控失敗，請查看Log檔。')
            return
        # There is/are missing sensors
        if error_sensors.__len__() != 0:
            logging.error(f'There are {error_sensors.__len__()} errors in Server Temperature')
            logging.error(f'Temperature trouble detail {error_sensors}')
            m_name = ", ".join([name for name in error_sensors.keys()])
            m_status = ", ".join([status for status in error_sensors.values()])
            self.__th_temp_error.humman_format(f'溫度感應器{m_name}', m_status, sampling_time, comp=self.SERVER)
            self.__th_temp_error.name = 'Server_Temp'
            if self.__th_temp_error.is_alive() is False:
                self.__th_temp_error.start()
                logging.debug(f'Continuous LINE Notify is started as [{self.__th_temp_error.name}] ID [{self.__th_temp_error.ident}]')
            else:
                self.__th_temp_error.trouble_occurs()
        else:
            if self.__th_temp_error.is_alive():
                self.__th_temp_error.humman_format('溫度感應器', 'OK', sampling_time, comp=self.SERVER, isHandled=True)
                self.__th_temp_error.repaired_completed()
        # Over temparature
        if high_temp.__len__() != 0:
            logging.error(f'There are {high_temp.__len__()} sensors is overheating')
            logging.error(f'High temperature sensor detail {high_temp}')
            m_name = ", ".join([name for name in high_temp.keys()])
            m_status = "目前溫度分別為" + ", ".join([f'{reading}°C' for reading in high_temp.values()])
            self.__th_temp_high.humman_format('溫度感應器{m_name}', m_status, sampling_time, f'溫度超過警告值的{self.OVER_TEMP_PERCENT}%', self.SERVER)
            self.__th_temp_high.name = 'Server_HighTemp'
            if self.__th_temp_high.is_alive() is False:
                self.__th_temp_high.start()
                logging.debug(f'Continuous LINE Notify is started as [{self.__th_temp_high.name}] ID [{self.__th_temp_high.ident}]')
            else:
                self.__th_temp_high.trouble_occurs()
        else:
            if self.__th_temp_high.is_alive():
                self.__th_temp_high.repaired_completed()
                self.__th_temp_high.humman_format('溫度感應器', '溫度正常', sampling_time, comp=self.SERVER, isHandled=True)
                self.__th_temp_high.send_notify()

    def power_EWS(self, power_data: dict, power_reading: dict):
        '''Early Warning System for power subsystem unusual'''
        sampling_time = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
        error_psu = {}
        try:
            s_reading = power_reading['present_power_reading']
            for psu_name, info in power_data.items():
                present = info['present']
                status = info['status']
                capacity = info['capacity']
                if status not in ('Good, In Use', 'Good, Standby', 'OK'):
                    error_psu[psu_name] = status
                    self.dbapi.execute_sql(
                        "INSERT INTO POWERSUPPLIES (SAMPLING_TIME, NAME, PRESENT, STATUS, READING, CAPACITY, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{psu_name}','{present}','{status}','{s_reading}','{capacity}',1)"
                    )
                else:
                    self.dbapi.execute_sql(
                        "INSERT INTO POWERSUPPLIES (SAMPLING_TIME, NAME, PRESENT, STATUS, READING, CAPACITY, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{psu_name}','{present}','{status}','{s_reading}','{capacity}',0)"
                    )
            if error_psu.__len__() != 0:
                logging.error(f'There are {error_psu.__len__()} errors in Server PSU')
                logging.error(f'Power Supply Unit detail {error_psu}')
                m_name = ", ".join([name for name in error_psu.keys()])
                m_status = ", ".join([status for status in error_psu.values()])
                self.__th_power.humman_format(f'電源{m_name}', m_status, sampling_time, comp=self.SERVER)
                self.__th_power.name = 'Server_Power'
                if self.__th_power.is_alive() is False:
                    self.__th_power.start()
                    logging.debug(f'Continuous LINE Notify is started as [{self.__th_power.name}] ID [{self.__th_power.ident}]')
                else:
                    self.__th_power.trouble_occurs()
            else:
                if self.__th_power.is_alive():
                    self.__th_power.humman_format('電源', 'OK', sampling_time, comp=self.SERVER, isHandled=True)
                    self.__th_power.repaired_completed()
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_power.send_notify(f'{self.SERVER}_Monitor]電源監控失敗，請查看Log檔。')
            return

    def memory_EWS(self, memory_data: dict):
        '''Early Warning System for memory subsystem unusual'''
        sampling_time = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
        failure_mem = {}
        try:
            summary = memory_data['memory_details_summary']
            detail = memory_data['memory_details']
            i_total_size = 0
            i_actual_size = 0
            for info in summary.values():
                if info['total_memory_size'] != 'N/A':
                    i_total_size = int(info['total_memory_size'].split(' ')[0])
            for cpu, slots in detail.items():
                for slot, info in slots.items():
                    status = info['status']
                    if status == 'Not Present':
                        continue
                    ram_type = info['type']
                    i_size = int(info['size'].split(' ')[0])
                    if status not in ('Present, Unused', 'Good, In Use'):
                        failure_mem[f'{slot}({cpu})'] = status
                        self.dbapi.execute_sql(
                            "INSERT INTO MEMORIES (SAMPLING_TIME, SLOT, CPU, STATUS, TYPE, SIZE, IS_ALARMED) "
                            f"VALUES('{sampling_time}','{slot}','{cpu}','{status}','{ram_type}','{i_size}',1)"
                        )
                    else:
                        self.dbapi.execute_sql(
                            "INSERT INTO MEMORIES (SAMPLING_TIME, SLOT, CPU, STATUS, TYPE, SIZE, IS_ALARMED) "
                            f"VALUES('{sampling_time}','{slot}','{cpu}','{status}','{ram_type}','{i_size}',0)"
                        )
                        if status == 'Good, In Use':
                            i_actual_size += i_size
            i_actual_size_gb = i_actual_size / 1024
            if i_actual_size_gb < i_total_size:
                unused = i_total_size - i_actual_size_gb
                logging.error(f'There are {unused}GB unused detected by Server MEM EWS')
                self.__th_memory.humman_format('Memory', f'{unused}GB Unused', sampling_time, '可用RAM容量跟現有容量比較少。', self.SERVER)
                self.__th_memory.send_notify()
            if failure_mem.__len__() != 0:
                logging.error(f'There are {failure_mem.__len__()} errors in Server MEM')
                logging.error(f'Power Supply Unit detail {failure_mem}')
                m_name = ", ".join([name for name in failure_mem.keys()])
                m_status = ", ".join([status for status in failure_mem.values()])
                self.__th_memory.humman_format(f'Memory {m_name}', m_status, sampling_time, comp=self.SERVER)
                self.__th_memory.name = 'Server_Mem'
                if self.__th_memory.is_alive() is False:
                    self.__th_memory.start()
                    logging.debug(f'Continuous LINE Notify is started as [{self.__th_memory.name}] ID [{self.__th_memory.ident}]')
                else:
                    self.__th_memory.trouble_occurs()
            else:
                if self.__th_memory.is_alive():
                    self.__th_memory.humman_format('Memory', 'Good, In Use', sampling_time, comp=self.SERVER, isHandled=True)
                    self.__th_memory.repaired_completed()
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_memory.send_notify(f'{self.SERVER}_Monitor]Server Memory監控失敗，請查看Log檔。')
            return

    def network_EWS(self, network_data: dict):
        '''Early Warning System for network subsystem unusual'''
        sampling_time = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
        net_down = {}
        try:
            for adap, info in network_data.items():
                port = info['network_port']
                status = info['status']
                desc = info['port_description']
                ip_add = info['ip_address']
                if status not in ('OK', 'Unknown'):
                    net_down[f'{adap}({port}-{desc})'] = status
                    self.dbapi.execute_sql(
                        "INSERT INTO NETWORK (SAMPLING_TIME, LOCATION, PORT, STATUS, DESCRIPTION, IP_ADDRESS, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{adap}','{port}','{status}','{desc}','{ip_add}',1)"
                    )
                else:
                    self.dbapi.execute_sql(
                        "INSERT INTO NETWORK (SAMPLING_TIME, LOCATION, PORT, STATUS, DESCRIPTION, IP_ADDRESS, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{adap}','{port}','{status}','{desc}','{ip_add}',0)"
                    )
            if net_down.__len__() != 0:
                logging.error(f'There are {net_down.__len__()} errors in Server NET')
                logging.error(f'Network detail {net_down}')
                m_name = ", ".join([name for name in net_down.keys()])
                m_status = ", ".join([status for status in net_down.values()])
                reason = self.describe(m_status.split(',')[0])
                self.__th_network.humman_format(f'Network Adapter {m_name}', m_status, sampling_time, reason, self.SERVER)
                self.__th_network.name = 'Server_Net'
                if self.__th_network.is_alive() is False:
                    self.__th_network.start()
                    logging.debug(f'Continuous LINE Notify is started as [{self.__th_network.name}] ID [{self.__th_network.ident}]')
                else:
                    self.__th_network.trouble_occurs()
            else:
                if self.__th_network.is_alive():
                    self.__th_network.humman_format('Network Adapter', 'OK', sampling_time, comp=self.SERVER, isHandled=True)
                    self.__th_network.repaired_completed()
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_network.send_notify('[SCC_Monitor]Server Network Adapter監控失敗，請查看Log檔。')
            return

    def storage_EWS(self, storage_data: dict):
        '''Early Warning System for storage subsystem unusual'''
        sampling_time = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
        controller = {}
        vir_disk = {}
        phy_disk = {}
        try:
            for info in storage_data.values():
                ct_name = info['label']
                ct_top_status = info['status']
                ct_status = info['controller_status']
                ct_cache_status = info['cache_module_status']
                ct_cache_mem = info['cache_module_memory']
                ct_model = info['model']
                if ct_top_status != 'OK' or ct_status != 'OK':
                    controller[ct_name] = f'{ct_top_status}, {ct_status}'
                    self.dbapi.execute_sql(
                        "INSERT INTO CONTROLLER (SAMPLING_TIME, NAME, STATUS, CONTROLLER_STATUS, CACHE_MODULE_STATUS, CACHE_MODULE_MEMORY, MODEL, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{ct_name}','{ct_top_status}','{ct_status}','{ct_cache_status}','{ct_cache_mem}','{ct_model}',1)"
                    )
                else:
                    self.dbapi.execute_sql(
                        "INSERT INTO CONTROLLER (SAMPLING_TIME, NAME, STATUS, CONTROLLER_STATUS, CACHE_MODULE_STATUS, CACHE_MODULE_MEMORY, MODEL, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{ct_name}','{ct_top_status}','{ct_status}','{ct_cache_status}','{ct_cache_mem}','{ct_model}',0)"
                    )
                for disk_info in info['logical_drives']:
                    vdisk_name = disk_info['label']
                    vdisk_status = disk_info['status']
                    vdisk_cap = disk_info['capacity']
                    raid_type = disk_info['fault_tolerance']
                    if vdisk_status != 'OK':
                        vir_disk[vdisk_name] = f'{vdisk_status}({raid_type})'
                        self.dbapi.execute_sql(
                            "INSERT INTO VDISK (SAMPLING_TIME, NAME, STATUS, CAPACITY, FAULT_TOLERANCE, IS_ALARMED) "
                            f"VALUES('{sampling_time}','{vdisk_name}','{vdisk_status}','{vdisk_cap}','{raid_type}',1)"
                        )
                    else:
                        self.dbapi.execute_sql(
                            "INSERT INTO VDISK (SAMPLING_TIME, NAME, STATUS, CAPACITY, FAULT_TOLERANCE, IS_ALARMED) "
                            f"VALUES('{sampling_time}','{vdisk_name}','{vdisk_status}','{vdisk_cap}','{raid_type}',0)"
                        )
                    for disk in disk_info['physical_drives']:
                        disk_name = disk['label']
                        disk_status = disk['status']
                        disk_cap = disk['capacity']
                        fw_ver = disk['fw_version']
                        serial = disk['serial_number']
                        if disk_status != 'OK':
                            phy_disk[disk_name] = disk_status
                            self.dbapi.execute_sql(
                                "INSERT INTO DISK (SAMPLING_TIME, NAME, STATUS, CAPACITY, FW_VERSION, SERIAL_NUMBER, IS_ALARMED) "
                                f"VALUES('{sampling_time}','{disk_name}','{disk_status}','{disk_cap}','{fw_ver}','{serial}',1)"
                            )
                        else:
                            self.dbapi.execute_sql(
                                "INSERT INTO DISK (SAMPLING_TIME, NAME, STATUS, CAPACITY, FW_VERSION, SERIAL_NUMBER, IS_ALARMED) "
                                f"VALUES('{sampling_time}','{disk_name}','{disk_status}','{disk_cap}','{fw_ver}','{serial}',0)"
                            )
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_controller.send_notify('[SCC_Monitor]Server硬碟監控失敗，請查看Log檔。')
            return
        if controller.__len__() != 0:
            logging.error(f'There are {controller.__len__()} errors in Server Controllers')
            logging.error(f'Controller trouble detail {controller}')
            name = controller.keys()
            stt = f'控制器狀態{controller.values()}'
            self.__th_controller.humman_format(f'控制器{name}', stt, sampling_time, comp=self.SERVER)
            self.__th_controller.name = 'Server_Control'
            if self.__th_controller.is_alive() is False:
                self.__th_controller.start()
                logging.debug(f'Continuous LINE Notify is started as [{self.__th_controller.name}] ID [{self.__th_controller.ident}]')
            else:
                self.__th_controller.trouble_occurs()
        else:
            if self.__th_controller.is_alive():
                self.__th_controller.humman_format('控制器', 'OK', sampling_time, isHandled=True, comp=self.SERVER)
                self.__th_controller.repaired_completed()

        if phy_disk.__len__() != 0:
            logging.error(f'There are {phy_disk.__len__()} errors in Server Disk')
            logging.error(f'Physical disk trouble detail {phy_disk}')
            m_name = ", ".join([name for name in phy_disk.keys()])
            m_status = ", ".join([status for status in phy_disk.values()])
            self.__th_disk.humman_format(f'硬碟{m_name}', m_status, sampling_time, comp=self.SERVER)
            self.__th_disk.name = 'Server_Disk'
            if self.__th_disk.is_alive() is False:
                self.__th_disk.start()
                logging.debug(f'Continuous LINE Notify is started as [{self.__th_disk.name}] ID [{self.__th_disk.ident}]')
            else:
                self.__th_disk.trouble_occurs()
        else:
            if self.__th_disk.is_alive():
                self.__th_disk.humman_format('硬碟', 'OK', sampling_time, isHandled=True, comp=self.SERVER)
                self.__th_disk.repaired_completed()

        if vir_disk.__len__() != 0:
            logging.error(f'There are {vir_disk.__len__()} errors in Server VDisk')
            logging.error(f'Virtual disk trouble detail {vir_disk}')
            m_name = ", ".join([name for name in vir_disk.keys()])
            m_status = ", ".join([status for status in vir_disk.values()])
            self.__th_vdisk.humman_format(f'虛擬磁盤{m_name}', m_status, sampling_time, comp=self.SERVER)
            self.__th_vdisk.name = 'Server_Vdisk'
            if self.__th_vdisk.is_alive() is False:
                self.__th_vdisk.start()
                logging.debug(f'Continuous LINE Notify is started as [{self.__th_vdisk.name}] ID [{self.__th_vdisk.ident}]')
            else:
                self.__th_vdisk.trouble_occurs()
        else:
            if self.__th_vdisk.is_alive():
                self.__th_vdisk.humman_format('虛擬磁盤', 'OK', sampling_time, isHandled=True, comp=self.SERVER)
                self.__th_vdisk.repaired_completed()


class HPMSA_Monitor():
    def __init__(self, api_ver=2) -> None:
        self.__api_ver = api_ver
        self.__generic_line = LineNotifyThread()
        self.__th_disk = LineNotifyThread()
        self.__th_vdisk = LineNotifyThread()
        self.__th_control = LineNotifyThread()
        self.__th_power = LineNotifyThread()
        self.apply_config()
        self.dbapi = SQLiteDB(db_dir=self.DB_DIR, db_name=self.DB_NAME, component='STORAGE')

    def apply_config(self):
        try:
            self.MSA_HOST = iniconfig.read('MSA_STORAGE', 'msa_host')
            self.MSA_LOGIN = iniconfig.read('MSA_STORAGE', 'msa_username')
            self.MSA_PASSWORD = iniconfig.read('MSA_STORAGE', 'msa_password')
            self.DB_DIR = iniconfig.read('MSA_STORAGE', 'history_database_location')
            self.DB_NAME = iniconfig.read('MSA_STORAGE', 'monitoring_history_file_name')

            logging.debug('=' * 80)
            logging.debug(f'MSA_STORAGE HOST [{self.MSA_HOST}] AUTHENTICATION [{self.MSA_LOGIN}@{self.MSA_PASSWORD}]')
            logging.debug(f'DATABASE set file {self.DB_NAME} in {self.DB_DIR}')
            logging.debug('=' * 80)
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')

    def make_cred_hash(self):
        cred = '_'.join([self.MSA_LOGIN, self.MSA_PASSWORD])
        hashed = md5(cred.encode()).hexdigest()
        return hashed

    def get_skey(self, use_cache=True):
        hashed_login = self.make_cred_hash()
        # Trying to use cached session key
        if use_cache:
            cur_timestamp = datetime.timestamp(datetime.now())
            cache_data = self.dbapi.execute_sql(f"SELECT EXPIRED, SKEY FROM SESSIONKEY WHERE IP='{self.MSA_HOST}' AND PROTO='http'")
            if cache_data is not None:
                cache_expired, cached_skey = cache_data
                if cur_timestamp < float(cache_expired):
                    return cached_skey
                else:
                    return self.get_skey(use_cache=False)
            else:
                return self.get_skey(use_cache=False)
        else:
            # Forming URL and trying to make GET query
            url = f'{self.MSA_HOST}/api/login/{hashed_login}'
            ret_code, sessionkey, xml = self.query_xmlapi(url=url, sessionkey=None)

            # 1 - success, write sessionkey to DB and return it
            if ret_code == '1':
                expired = datetime.timestamp(datetime.now() + timedelta(minutes=30))
                cache_data = self.dbapi.execute_sql(f"SELECT IP FROM SESSIONKEY WHERE IP = '{self.MSA_HOST}' AND PROTO='http'")
                if cache_data is None:
                    self.dbapi.execute_sql(f"INSERT INTO SESSIONKEY (IP, PROTO, EXPIRED, SKEY) VALUES ('{self.MSA_HOST}', 'http', '{expired}', '{sessionkey}')")
                else:
                    self.dbapi.execute_sql(f"UPDATE SESSIONKEY SET SKEY='{sessionkey}', EXPIRED='{expired}' WHERE IP='{self.MSA_HOST}' AND PROTO='http'")
                return sessionkey
            # 2 - Authentication Unsuccessful, return "2"
            elif ret_code == '2':
                return ret_code

    def query_xmlapi(self, url: str, sessionkey):
        # Makes GET request to URL
        try:
            # Connection timeout in seconds (connection, read).
            timeout = (3, 10)
            full_url = 'http://' + url
            headers = {'sessionKey': sessionkey} if self.__api_ver == 2 else {'Cookie': "wbiusername={}; wbisessionkey={}"}
            response = requests.get(url=full_url, headers=headers, timeout=timeout)
            logging.info(f'GET "{full_url}" {response.status_code}')
        except requests.RequestException:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__generic_line.send_notify(f'[SCC Monitor]連不到{full_url}，請查看IP地址、帳號或密碼。')

        # Reading data from server XML response
        try:
            response_xml = ET.fromstring(response.content)
            return_type = response_xml.find("./OBJECT[@name='status']/PROPERTY[@name='response-type-numeric']").text
            return_code = response_xml.find("./OBJECT[@name='status']/PROPERTY[@name='return-code']").text
            return_response = response_xml.find("./OBJECT[@name='status']/PROPERTY[@name='response']").text
            if return_code == '0' or return_type == '0':
                logging.info(f'CALL API SUCCESS "{url.split("/")[-1]}" "{return_code}" "{return_response}"')
            else:
                logging.error(f'CALL API FAILED "{url.split("/")[-1]}" "{return_code}" "{return_response}"')
                self.__generic_line.send_notify(f'[SCC Monitor]調用API接口失敗{full_url}，原因為{return_response}')
            return return_code, return_response, response_xml
        except (ValueError, AttributeError) as e:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')

    def get_storage_health(self):
        logging.info(f'Connecting to StorageWorks P2000 MSA, host {self.MSA_HOST}.')
        # CLI component names to XML API mapping
        comp_names_map = {
            'disks': 'drive', 'vdisks': 'virtual-disk', 'controllers': 'controllers',
            'power-supplies': 'power-supplies', 'controller-statistics': 'controller-statistics', 'disk-statistics': 'disk-statistics',
        }
        comp_function_map = {
            'disks': 'phy_disk_EWS', 'vdisks': 'vr_disk_EWS', 'controllers': 'controllers_EWS',
            'power-supplies': 'power_EWS', 'controller-statistics': 'controller_statistics', 'disk-statistics': 'disk_statistics',
        }
        try:
            sessionkey = self.get_skey()
            prop_parts: List[ET.Element]
            for comp, obj_name in comp_names_map.items():
                logging.debug(f'Shows information about all {comp} in the storage system.')
                url = f'{self.MSA_HOST}/api/show/{comp}'
                # Making request to API
                resp_return_code, resp_description, xml_elements = self.query_xmlapi(url, sessionkey)
                if resp_return_code != '0':
                    continue
                prop_parts = xml_elements.findall("./OBJECT[@name='{}']".format(obj_name))
                EWS_function = getattr(self.__class__, comp_function_map[comp])
                EWS_function(self, prop_parts)
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            return

    def phy_disk_EWS(self, properties: List[ET.Element]):
        sampling_time = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
        failed_disk = {}
        try:
            for PROP in properties:
                drive = {}
                # Processing main properties
                location = PROP.find("./PROPERTY[@name='durable-id']").text
                status = PROP.find("./PROPERTY[@name='status']").text
                health = PROP.find("./PROPERTY[@name='health']").text
                health_reason = PROP.find("./PROPERTY[@name='health-reason']").text
                how_used = PROP.find("./PROPERTY[@name='state']").text
                # Processing advanced properties
                current_job = PROP.find("./PROPERTY[@name='job-running-numeric']").text
                io_resp_time = PROP.find("./PROPERTY[@name='avg-rsp-time']").text
                data_transf = PROP.find("./PROPERTY[@name='total-data-transferred']").text
                if status != 'Up' or health != 'OK':
                    drive['status'] = status
                    drive['health'] = health
                    drive['health_reason'] = health_reason
                    failed_disk[location] = drive
                    self.dbapi.execute_sql(
                        "INSERT INTO DISK (SAMPLING_TIME, LOCATION, STATUS, HEALTH, HEALTH_REASON, HOW_USED, CURRENT_JOB, DATA_TRANSFERRED, AVG_RSP_TIME, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{location}','{status}','{health}','{health_reason}','{how_used}','{current_job}','{data_transf}','{io_resp_time}',1)"
                    )
                else:
                    self.dbapi.execute_sql(
                        "INSERT INTO DISK (SAMPLING_TIME, LOCATION, STATUS, HEALTH, HEALTH_REASON, HOW_USED, CURRENT_JOB, DATA_TRANSFERRED, AVG_RSP_TIME, IS_ALARMED) "
                        f"VALUES ('{sampling_time}','{location}','{status}','{health}','{health_reason}','{how_used}','{current_job}','{data_transf}','{io_resp_time}',0)"
                    )
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_disk.send_notify('[SCC_Monitor]硬碟監控失敗，請查看Log檔。')
        if failed_disk.__len__() != 0:
            logging.error(f'There are {failed_disk.__len__()} errors in StorageWorks Physical Disk')
            logging.error(f'StorageWorks Physical disk trouble detail {failed_disk}')
            name = ', '.join([drive for drive in failed_disk.keys()])
            state = ', '.join([f"[{info['status']},{info['health']}]" for _, info in failed_disk.items()])
            reason = next(iter([info['health_reason'] for _, info in failed_disk.items()]))
            self.__th_disk.humman_format(f'硬碟{name}', state, sampling_time, reason, comp='Storage')
            self.__th_disk.name = 'StorageWorks_Disk'
            if self.__th_disk.is_alive() is False:
                self.__th_disk.start()
                logging.debug(f'Continuous LINE Notify is started as [{self.__th_disk.name}] ID [{self.__th_disk.ident}]')
            else:
                self.__th_disk.trouble_occurs()
        else:
            if self.__th_disk.is_alive():
                self.__th_disk.humman_format('硬碟', 'OK', sampling_time, comp='Storage', isHandled=True)
                self.__th_disk.repaired_completed()

    def vr_disk_EWS(self, properties: List[ET.Element]):
        sampling_time = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
        failed_vdisk = {}
        try:
            for PROP in properties:
                drive = {}
                # Processing main properties
                name = PROP.find("./PROPERTY[@name='name']").text
                owner = PROP.find("./PROPERTY[@name='owner']").text
                status = PROP.find("./PROPERTY[@name='status']").text
                health = PROP.find("./PROPERTY[@name='health']").text
                health_reason = PROP.find("./PROPERTY[@name='health-reason']").text
                # Processing advanced properties
                raid_type = PROP.find("./PROPERTY[@name='raidtype']").text
                current_job = PROP.find("./PROPERTY[@name='current-job']").text
                disk_count = PROP.find("./PROPERTY[@name='diskcount']").text + PROP.find("./PROPERTY[@name='sparecount']").text
                create_date = PROP.find("./PROPERTY[@name='create-date']").text
                reads_count = PROP.find("./PROPERTY[@name='number-of-reads']").text
                writes_count = PROP.find("./PROPERTY[@name='number-of-writes']").text
                data_transf = PROP.find("./PROPERTY[@name='total-data-transferred']").text
                if status != 'FTOL' or health != 'OK':
                    drive['status'] = status
                    drive['health'] = health
                    drive['health_reason'] = health_reason
                    failed_vdisk[f'{name}({raid_type})'] = drive
                    self.dbapi.execute_sql(
                        "INSERT INTO VDISK (SAMPLING_TIME, NAME, OWNER, STATUS, HEALTH, HEALTH_REASON, RAID_TYPE, CURRENT_JOB, DISK_COUNT, CREATE_DATE, READS_COUNT, WRITES_COUNT, DATA_TRANSFERRED, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{name}','{owner}','{status}','{health}','{health_reason}','{raid_type}','{current_job}','{disk_count}','{create_date}','{reads_count}','{writes_count}','{data_transf}',1)"
                    )
                else:
                    self.dbapi.execute_sql(
                        "INSERT INTO VDISK (SAMPLING_TIME, NAME, OWNER, STATUS, HEALTH, HEALTH_REASON, RAID_TYPE, CURRENT_JOB, DISK_COUNT, CREATE_DATE, READS_COUNT, WRITES_COUNT, DATA_TRANSFERRED, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{name}','{owner}','{status}','{health}','{health_reason}','{raid_type}','{current_job}','{disk_count}','{create_date}','{reads_count}','{writes_count}','{data_transf}',0)"
                    )
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_vdisk.send_notify('[SCC_Monitor]Storage虛擬磁盤監控失敗，請查看Log檔。')
        if failed_vdisk.__len__() != 0:
            logging.error(f'There are {failed_vdisk.__len__()} errors in StorageWorks Virtual Disk')
            logging.error(f'StorageWorks Virtual disk trouble detail {failed_vdisk}')
            name = ', '.join([drive for drive in failed_vdisk.keys()])
            state = ', '.join([f"[{info['status']},{info['health']}]" for _, info in failed_vdisk.items()])
            reason = next(iter([info['health_reason'] for _, info in failed_vdisk.items()]))
            self.__th_vdisk.humman_format(f'虛擬磁盤{name}', state, sampling_time, reason, 'Storage')
            self.__th_vdisk.name = 'StorageWorks_Vdisk'
            if self.__th_vdisk.is_alive() is False:
                self.__th_vdisk.start()
                logging.debug(f'Continuous LINE Notify is started as [{self.__th_vdisk.name}] ID [{self.__th_vdisk.ident}]')
            else:
                self.__th_vdisk.trouble_occurs()
        else:
            if self.__th_vdisk.is_alive():
                self.__th_vdisk.humman_format('虛擬磁盤', 'OK', sampling_time, comp='Storage', isHandled=True)
                self.__th_vdisk.repaired_completed()

    def controllers_EWS(self, properties: List[ET.Element]):
        sampling_time = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
        bad_controllers = {}
        try:
            for PROP in properties:
                controller = {}
                # Processing main properties
                name = PROP.find("./PROPERTY[@name='durable-id']").text
                status = PROP.find("./PROPERTY[@name='status']").text
                health = PROP.find("./PROPERTY[@name='health']").text
                health_reason = PROP.find("./PROPERTY[@name='health-reason']").text
                redundancy_status = PROP.find("./PROPERTY[@name='redundancy-status']").text
                # Processing advanced properties
                ip_address = PROP.find("./PROPERTY[@name='ip-address']").text
                failed_over = PROP.find("./PROPERTY[@name='failed-over']").text
                fail_over_reason = PROP.find("./PROPERTY[@name='fail-over-reason']").text
                mfg_date = PROP.find("./PROPERTY[@name='mfg-date']").text
                position = PROP.find("./PROPERTY[@name='position']").text
                redundancy_mode = PROP.find("./PROPERTY[@name='redundancy-mode']").text
                if status != 'Operational' or health not in ('OK', 'Degraded'):
                    controller['status'] = status
                    controller['health'] = health
                    controller['health_reason'] = health_reason
                    bad_controllers[name] = controller
                    self.dbapi.execute_sql(
                        "INSERT INTO CONTROLLER (SAMPLING_TIME, NAME, IP_ADDRESS, STATUS, HEALTH, HEALTH_REASON, FAILED_OVER, FAILED_OVER_REASON, MFG_DATE, POSITION, REDUNDANCY_MODE, REDUNDANCY_STATUS, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{name}','{ip_address}','{status}','{health}','{health_reason}','{failed_over}','{fail_over_reason}','{mfg_date}','{position}','{redundancy_mode}','{redundancy_status}',1)"
                    )
                else:
                    self.dbapi.execute_sql(
                        "INSERT INTO CONTROLLER (SAMPLING_TIME, NAME, IP_ADDRESS, STATUS, HEALTH, HEALTH_REASON, FAILED_OVER, FAILED_OVER_REASON, MFG_DATE, POSITION, REDUNDANCY_MODE, REDUNDANCY_STATUS, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{name}','{ip_address}','{status}','{health}','{health_reason}','{failed_over}','{fail_over_reason}','{mfg_date}','{position}','{redundancy_mode}','{redundancy_status}',0)"
                    )
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_control.send_notify('[SCC_Monitor]Storage控制器監控失敗，請查看Log檔。')
        if bad_controllers.__len__() != 0:
            logging.error(f'There are {bad_controllers.__len__()} errors in StorageWorks Controllers')
            logging.error(f'StorageWorks Controller trouble detail {bad_controllers}')
            name = ', '.join([ct for ct in bad_controllers.keys()])
            state = '; '.join([f"{info['status']}, {info['health']}" for _, info in bad_controllers.items()])
            reason = next(iter([info['health_reason'] for _, info in bad_controllers.items()]))
            self.__th_control.humman_format(f'控制器{name}', state, sampling_time, reason, 'Storage')
            self.__th_control.name = 'StorageWorks_Controller'
            if self.__th_control.is_alive() is False:
                self.__th_control.start()
                logging.debug(f'Continuous LINE Notify is started as [{self.__th_control.name}] ID [{self.__th_control.ident}]')
            else:
                self.__th_control.trouble_occurs()
        else:
            if self.__th_control.is_alive():
                self.__th_control.humman_format('控制器', 'Operational', sampling_time, None, 'Storage', isHandled=True)
                self.__th_control.repaired_completed()

    def power_EWS(self, properties: List[ET.Element]):
        sampling_time = datetime.now().__format__('%Y-%m-%d %H:%M:%S')
        failure_units = {}
        try:
            for PROP in properties:
                psu = {}
                fan_bays = {}
                # Processing main properties
                name = PROP.find("./PROPERTY[@name='name']").text
                status = PROP.find("./PROPERTY[@name='status']").text
                health = PROP.find("./PROPERTY[@name='health']").text
                health_reason = PROP.find("./PROPERTY[@name='health-reason']").text
                # Processing advanced properties
                dc12v = PROP.find("./PROPERTY[@name='dc12v']").text
                dc5v = PROP.find("./PROPERTY[@name='dc5v']").text
                dc33v = PROP.find("./PROPERTY[@name='dc33v']").text
                dc12i = PROP.find("./PROPERTY[@name='dc12i']").text
                dc5i = PROP.find("./PROPERTY[@name='dc5i']").text
                psu_temp = PROP.find("./PROPERTY[@name='dctemp']").text
                mfg_date = PROP.find("./PROPERTY[@name='mfg-date']").text
                # Processing Fan of PSU
                fan_name = PROP.find("./OBJECT[@name='fan-details']/PROPERTY[@name='name']").text
                fan_status = PROP.find("./OBJECT[@name='fan-details']/PROPERTY[@name='status']").text
                fan_speed = PROP.find("./OBJECT[@name='fan-details']/PROPERTY[@name='speed']").text
                fan_health = PROP.find("./OBJECT[@name='fan-details']/PROPERTY[@name='health']").text
                fan_health_reason = PROP.find("./OBJECT[@name='fan-details']/PROPERTY[@name='health-reason']").text
                if fan_status != 'Up' or fan_health != 'OK':
                    fan_bays['status'] = fan_status
                    fan_bays['health'] = fan_health
                    fan_bays['health_reason'] = fan_health_reason
                    failure_units[fan_name] = fan_bays

                if status != 'Up' or health != 'OK':
                    psu['status'] = status
                    psu['health'] = health
                    psu['health_reason'] = health_reason
                    failure_units[name] = psu
                    self.dbapi.execute_sql(
                        "INSERT INTO POWERSUPPLIES (SAMPLING_TIME, NAME, STATUS, HEALTH, HEALTH_REASON, DC12V, DC5V, DC33V, DC12I, DC5I, PSU_TEMP, MFG_DATE, FAN_NAME, FAN_STATUS, FAN_SPEED_RPM, FAN_HEALTH, FAN_HEALTH_REASON, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{name}','{status}','{health}','{health_reason}','{dc12v}','{dc5v}','{dc33v}','{dc12i}','{dc5i}','{psu_temp}','{mfg_date}','{fan_name}','{fan_status}','{fan_speed}','{fan_health}','{fan_health_reason}',1)"
                    )
                else:
                    self.dbapi.execute_sql(
                        "INSERT INTO POWERSUPPLIES (SAMPLING_TIME, NAME, STATUS, HEALTH, HEALTH_REASON, DC12V, DC5V, DC33V, DC12I, DC5I, PSU_TEMP, MFG_DATE, FAN_NAME, FAN_STATUS, FAN_SPEED_RPM, FAN_HEALTH, FAN_HEALTH_REASON, IS_ALARMED) "
                        f"VALUES('{sampling_time}','{name}','{status}','{health}','{health_reason}','{dc12v}','{dc5v}','{dc33v}','{dc12i}','{dc5i}','{psu_temp}','{mfg_date}','{fan_name}','{fan_status}','{fan_speed}','{fan_health}','{fan_health_reason}',0)"
                    )
        except AttributeError as e:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')
            self.__th_power.send_notify('[SCC_Monitor]Storage電源監控失敗，請查看Log檔。')
        if failure_units.__len__() != 0:
            logging.error(f'There are {failure_units.__len__()} errors in StorageWorks PSU')
            logging.error(f'StorageWorks Controller trouble detail {failure_units}')
            name = '; '.join([unit for unit in failure_units.keys()])
            state = ', '.join([f"[{info['status']},{info['health']}]" for _, info in failure_units.items()])
            reason = next(iter([info['health_reason'] for _, info in failure_units.items()]))
            self.__th_power.humman_format(f'電源{name}', state, sampling_time, reason, 'Storage')
            self.__th_power.name = 'StorageWorks_PSU'
            if self.__th_power.is_alive() is False:
                self.__th_power.start()
                logging.debug(f'Continuous LINE Notify is started as [{self.__th_control.name}] ID [{self.__th_control.ident}]')
            else:
                self.__th_power.trouble_occurs()
        else:
            if self.__th_power.is_alive():
                self.__th_power.humman_format('電源', 'OK', sampling_time, None, 'Storage', isHandled=True)
                self.__th_power.repaired_completed()

    def controller_statistics(self, properties: List[ET.Element]):
        for PROP in properties:
            name = PROP.find("./PROPERTY[@name='durable-id']").text
            power_on_time = PROP.find("./PROPERTY[@name='power-on-time']").text
            read_count = PROP.find("./PROPERTY[@name='number-of-reads']").text
            writes_count = PROP.find("./PROPERTY[@name='number-of-writes']").text
            data_read = PROP.find("./PROPERTY[@name='data-read']").text
            data_written = PROP.find("./PROPERTY[@name='data-written']").text
            reset_time = PROP.find("./PROPERTY[@name='reset-time']").text
            total_power_on_hours = PROP.find("./PROPERTY[@name='total-power-on-hours']").text
            self.dbapi.execute_sql(
                f"UPDATE CONTROLLER SET power_on_time='{power_on_time}', reads_count='{read_count}', writes_count='{writes_count}', data_read='{data_read}', "
                f"data_written='{data_written}', reset_time='{reset_time}', total_power_on_hours='{total_power_on_hours}' WHERE name='{name.lower()}'"
            )

    def disk_statistics(self, properties: List[ET.Element]):
        for PROP in properties:
            name = PROP.find("./PROPERTY[@name='durable-id']").text
            iops = PROP.find("./PROPERTY[@name='iops']").text
            read_count = PROP.find("./PROPERTY[@name='number-of-reads']").text
            writes_count = PROP.find("./PROPERTY[@name='number-of-writes']").text
            data_read = PROP.find("./PROPERTY[@name='data-read']").text
            data_written = PROP.find("./PROPERTY[@name='data-written']").text
            reset_time = PROP.find("./PROPERTY[@name='reset-time']").text
            self.dbapi.execute_sql(
                f"UPDATE DISK SET iops='{iops}', reads_count='{read_count}', writes_count='{writes_count}', data_read='{data_read}', "
                f"data_written='{data_written}', reset_time='{reset_time}' WHERE location='{name}'"
            )


class SCCService(ServiceBase):
    # Define service information
    _svc_name_ = 'SCCMonitor'
    _svc_display_name_ = 'SCC Health Monitor'
    _svc_description_ = 'SCC health monitor, send LINE notify to L2 member group when trouble occurred'

    def __init__(self, args):
        super().__init__(args)
        self._mailer = LineNotifyThread()
        self._is_service_run = False
        self.apply_config()

    def apply_config(self):
        try:
            self.INTERVAL_TIME = int(iniconfig.read('SETTING', 'monitoring_interval_hour')) * 3600
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')

    def description(self):
        logging.debug('=' * 80)
        logging.debug(f'SERVICE NAME: {self._svc_name_}')
        logging.debug(f'DISPLAY NAME: {self._svc_display_name_}')
        logging.debug(f'DESCRIPTION : {self._svc_description_}')
        logging.debug(f'INTERVAL_TIME : {self.INTERVAL_TIME}')
        logging.debug('=' * 80)

    def start(self):
        self.description()
        self._is_service_run = True
        logging.debug('=' * 80)
        logging.info(f'STARTING SERVICE {self._svc_name_.upper()}')
        self._mailer.send_notify(f'熱軋主線{self._svc_name_}服務已啟動成功')
        logging.debug('=' * 80)

    def stop(self):
        self._is_service_run = False
        logging.debug('=' * 80)
        self._mailer.send_notify(f'熱軋主線{self._svc_name_}服務被關閉。請知悉！')
        logging.error(f'STOPPING SERVICE {self._svc_name_.upper()}')
        logging.debug('=' * 80)

    def main(self):
        try:
            startTime = time()
            ilo_scc01 = HPiLO_Monitor(server='L2SCC01')
            ilo_scc02 = HPiLO_Monitor(server='L2SCC02')
            msa = HPMSA_Monitor()
            while self._is_service_run:
                ilo_scc01.get_server_health()
                ilo_scc02.get_server_health()
                msa.get_storage_health()
                logging.debug('Monitoring ProLiant server and StorageWorks completed. Waiting for next loop.')
                sleep(self.INTERVAL_TIME - ((time() - startTime) % self.INTERVAL_TIME))
        except Exception:
            exc_type, exc_val, exc_tb = sys.exc_info()
            throw_exc = traceback.extract_tb(exc_tb)[0]
            logging.error(f'{exc_type.__name__} in {throw_exc} cause {exc_val}')


if __name__ == '__main__':
    ConfigLogger()
    iniconfig = INIConfiguration()
    if len(sys.argv) == 1:  # by pass error 1503
        servicemanager.Initialize()
        servicemanager.PrepareToHostSingle(SCCService)
        servicemanager.StartServiceCtrlDispatcher()
    else:
        SCCService.parse_command_line(SCCService)
