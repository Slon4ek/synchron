import datetime
import os
from typing import Dict

import requests
from loguru import logger


class YandexConnector:
    """
    Класс коннектор для работы с Яндекс Диском
    """

    def __init__(self, token, cloud_dir, main_url):
        self._token = token
        self._dir_name = cloud_dir
        self._headers = {'Authorization': f'OAuth {self._token}'}
        self._main_url = main_url
        self._endpoints = {
            'disk_info': 'v1/disk/',
            'files': 'v1/disk/resources',
            'last_uploaded': 'v1/disk/resources/last-uploaded',
            'upload': 'v1/disk/resources/upload'
        }

    def __main_get_request(self, endpoint: str, params: Dict | None = None) -> requests.Response:
        """
        Основной GET-зарос
        :param endpoint: строка endpoint к основному URL
        :param params: параметры запроса
        :return: GET-запрос
        """
        if params is None:
            params = dict()
        headers = self._headers
        return requests.get(
            url=f'{self._main_url}{endpoint}',
            headers=headers,
            params=params,
            timeout=10
        )

    def __main_delete_request(self, endpoint: str, params: Dict | None = None) -> requests.Response:
        """
        Основной DELETE-запрос
        :param endpoint: строка endpoint к основному URL
        :param params: параметры запроса
        :return: DELETE-запрос
        """
        if params is None:
            params = dict()
        headers = self._headers
        return requests.delete(
            url=f'{self._main_url}{endpoint}',
            headers=headers,
            params=params,
            timeout=10
        )

    def get_disk_info(self) -> Dict:
        """
        Метод для получения информации о Диске
        :return: словарь
        """
        request = self.__main_get_request(self._endpoints['disk_info'])
        if request.status_code == requests.codes.ok:
            disk_info = {'Объем диска': request.json()['total_space'],
                         'Занято': request.json()['used_space'],
                         'Свободно': request.json()['total_space'] - request.json()['used_space']
                         }
            return disk_info
        else:
            logger.error(request.json()['message'])

    def get_files_info(self) -> Dict:
        """
        Метод возвращает информацию о файлах хранящихся в указанной директории Яндекс Диска.
        :return: Словарь ключ: Имя файла, значение: дата последнего изменения
        """
        params = {
            'path': self._dir_name,
            'fields': 'name, _embedded.items.name, _embedded.items.modified',
            'limit': 10000
        }
        request = self.__main_get_request(self._endpoints['files'], params)
        if request.status_code == requests.codes.ok:
            request = request.json()
            file_dict = dict()
            for file in request['_embedded']['items']:
                file_dict[file['name']] = file['modified'][:19]
            return file_dict
        else:
            logger.error(request.json()['message'])
            exit()

    def load(self, file_path: str) -> None:
        """
        Метод записи/перезаписи файлов на Яндекс Диск
        :param file_path: путь к загружаемому файлу на компьютере пользователя
        :return: None
        """
        file_name = os.path.basename(file_path)
        params = {
            'path': self._dir_name + file_name,
            'overwrite': True
        }
        request = self.__main_get_request(self._endpoints['upload'], params)
        if request.status_code == requests.codes.ok:
            request = request.json()
            with open(file_path, 'rb') as file:
                requests.put(request['href'], files={'file': file})
        else:
            logger.error(request.json()['message'])

    def delete(self, file_name: str) -> None:
        """
        Метод удаления файла с хранилища Яндекс Диска
        :param file_name: имя удаляемого файла
        :return: None
        """
        params = {'path': self._dir_name + file_name}
        request = self.__main_delete_request(self._endpoints['files'], params)
        if request.status_code != 204:
            logger.error(request.json()['message'])
