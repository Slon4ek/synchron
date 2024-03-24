import os
import time
from datetime import datetime

import dotenv
from dotenv import find_dotenv, load_dotenv
from loguru import logger

from connector import YandexConnector


def synchronization(cloud_obj: YandexConnector, tracked_directory: str) -> None:
    """
    Функция синхронизации файлов
    :param cloud_obj: объект класса работы с хранилищем
    :param tracked_directory: отслеживаемая директория на компьютере пользователя
    :return: None
    """

    try:
        files_in_disk = cloud_obj.get_files_info()
        files_in_dir = os.listdir(tracked_directory)
        add_count = 0
        delete_count = 0
        for file_name in files_in_dir:
            if file_name not in files_in_disk.keys():
                cloud_obj.load(os.path.join(tracked_directory, file_name))
                logger.info(f'{str(datetime.now())[:19]}: Файл {file_name} успешно загружен на Диск')
                add_count += 1

        for file_name, m_date_file_in_disk in files_in_disk.items():
            if file_name not in files_in_dir:
                cloud_obj.delete(file_name)
                logger.info(f'{str(datetime.now())[:19]}: Файл {file_name} успешно удален')
                delete_count += 1
                continue
            m_date_file_in_dir = datetime.utcfromtimestamp(
                os.path.getmtime(os.path.join(tracked_directory, file_name))
            ).isoformat()
            if file_name in files_in_dir and m_date_file_in_dir > m_date_file_in_disk:
                cloud_obj.load(os.path.join(tracked_directory, file_name))
                logger.info(f'{str(datetime.now())[:19]}: Файл {file_name} успешно перезаписан')
                add_count += 1
        else:
            logger.info(f'{str(datetime.now())[:19]}: Загружено {add_count} файлов. Удалено {delete_count} файлов')
    except (FileNotFoundError, FileExistsError) as exc:
        logger.error(exc)
        exit()
    except Exception as exc:
        logger.error(exc)


def main() -> None:
    """
    Основная функция запуска программы. Загружает переменные окружения, создает объект класса для
    работы с API, запускает цикл синхронизации
    :return: None
    """
    if not find_dotenv():
        exit('Переменные окружения не загружены. Отсутствует файл .env')
    else:
        load_dotenv()
        config = dotenv.dotenv_values()
    logger.add(config['INFO_LOG_PATH'], level='INFO', rotation='1 day', compression='zip')
    logger.add(config['ERROR_LOG_PATH'], level='ERROR', rotation='1 day', compression='zip')
    my_disk = YandexConnector(token=config['YANDEX_API_TOKEN'],
                              cloud_dir=config['CLOUD_DIR_PATH'],
                              main_url=config['MAIN_URL']
                              )
    tracked_directory = os.path.abspath(os.path.join(os.sep, config['SYNCH_DIR']))
    logger.info(f'{datetime.now()}: Программа синхронизации файлов начала работу с директорией {tracked_directory}')
    while True:
        synchronization(my_disk, tracked_directory)
        time.sleep(10)
