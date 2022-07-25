# -*- coding: utf-8 -*-


# Вычисление 3-х тикеров с максимальной и 3-х тикеров с минимальной волатильностью в МНОГОПОТОЧНОМ стиле


import os
from utils import time_track
import threading

SOURCE_PATH = 'trades'

main_dict = {}
tickers_max_volatility = {}
tickers_min_volatility = {}
zero_volatility = []
lock = threading.Lock()


class CalculationOfVolatility(threading.Thread):

    def __init__(self, dirpath, files, main_data_dict, lock, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dirpath = dirpath
        self.files = files
        self.main_data_dict = main_data_dict
        self.main_data_dict_lock = lock

    def run(self):
        for name in self.files:
            file_path = os.path.join(self.dirpath, name)
            self.getting_data_from_file(file=file_path)

    def getting_data_from_file(self, file):
        """Получение данных из файлов.

        Прохождение по директориям и файлам и создание конечных путей файлов (file_path).
        Чтение файла, заполнение списка цен ТИКЕРА (list_of_ticker_prices) для нахождения максимума и минимума.
        Рассчёт волатильности ТИКЕРА и запись в главный словарь (self.main_dict), для последующего распределения."""

        with open(file, 'r', encoding='UTF-8') as ff:
            list_of_ticker_prices = []
            for line in ff:
                row_data = line.split(',')
                if row_data[2] != 'PRICE':
                    list_of_ticker_prices.append(float(row_data[2]))
            max_price = max(list_of_ticker_prices)
            min_price = min(list_of_ticker_prices)
            average_price = (max_price + min_price) / 2
            volatility = ((max_price - min_price) / average_price) * 100
            self.main_data_dict_lock.acquire()
            self.main_data_dict[row_data[0]] = round(volatility, 3)
            self.main_data_dict_lock.release()
            #  лок


def get_max_max_zero_tickers_volatility():
    for key, value in main_dict.items():
        if value == 0.0:
            zero_volatility.append(key)
    for j in zero_volatility:
        del main_dict[j]
    zero_volatility.sort()
    count = 0
    while count < 3:
        max_volatility = max(main_dict.values())
        for key, value in main_dict.items():
            if max_volatility == value:
                tickers_max_volatility[key] = value
                del main_dict[key]
                break
        count += 1
    while 2 < count < 6:
        min_volatility = min(main_dict.values())
        for key, value in main_dict.items():
            if min_volatility == value:
                tickers_min_volatility[key] = value
                del main_dict[key]
                break
        count += 1

    print('Максимальная волатильность:')
    for key, value in tickers_max_volatility.items():
        print(f'{key} - {value} %')
    print('Минимальная волатильность:')
    for key, value in tickers_min_volatility.items():
        print(f'{key} - {value} %')
    print('Нулевая волатильность: \n', ', '.join(zero_volatility))


@time_track
def main():
    calculation_volatility = [
        CalculationOfVolatility(dirpath=dirpath, files=files, main_data_dict=main_dict, lock=lock) for
        dirpath, dirname, files in os.walk(SOURCE_PATH)]
    for calc in calculation_volatility:
        calc.start()
    for calc in calculation_volatility:
        calc.join()

    get_max_max_zero_tickers_volatility()


if __name__ == '__main__':
    main()

