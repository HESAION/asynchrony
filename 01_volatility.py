# -*- coding: utf-8 -*-


# Описание предметной области:
#
# При торгах на бирже совершаются сделки - один купил, второй продал.
# Покупают и продают ценные бумаги (акции, облигации, фьючерсы, етс). Ценные бумаги - это по сути долговые расписки.
# Ценные бумаги выпускаются партиями, от десятка до несколько миллионов штук.
# Каждая такая партия (выпуск) имеет свой торговый код на бирже - тикер - https://goo.gl/MJQ5Lq
# Все бумаги из этой партии (выпуска) одинаковы в цене, поэтому говорят о цене одной бумаги.
# У разных выпусков бумаг - разные цены, которые могут отличаться в сотни и тысячи раз.
# Каждая биржевая сделка характеризуется:
#   тикер ценнной бумаги
#   время сделки
#   цена сделки
#   обьем сделки (сколько ценных бумаг было куплено)
#
# В ходе торгов цены сделок могут со временем расти и понижаться. Величина изменения цен называтея волатильностью.
# Например, если бумага №1 торговалась с ценами 11, 11, 12, 11, 12, 11, 11, 11 - то она мало волатильна.
# А если у бумаги №2 цены сделок были: 20, 15, 23, 56, 100, 50, 3, 10 - то такая бумага имеет большую волатильность.
# Волатильность можно считать разными способами, реализован подсчет сильно упрощенным способом -
# отклонение в процентах от средней цены за торговую сессию:
#   средняя цена = (максимальная цена + минимальная цена) / 2
#   волатильность = ((максимальная цена - минимальная цена) / средняя цена) * 100%
#
# Например для бумаги №1:
#   average_price = (12 + 11) / 2 = 11.5
#   volatility = ((12 - 11) / average_price) * 100 = 8.7%
# Для бумаги №2:
#   average_price = (100 + 3) / 2 = 51.5
#   volatility = ((100 - 3) / average_price) * 100 = 188.34%
#
#
# Нужно вычислить 3 тикера с максимальной и 3 тикера с минимальной волатильностью.
#
import zipfile
import os
from pprint import pprint
from utils import time_track

"""Для начала стоит вытащить файлы из архива"""
# file_name = 'trades.zip'
#
# zfile = zipfile.ZipFile(file_name, 'r')
# for filename in zfile.namelist():
#     zfile.extract(filename, path=filename)


SOURCE_PATH = 'trades'

main_dict = {}
tickers_max_volatility = {}
tickers_min_volatility = {}
zero_volatility = []


class CalculationOfVolatility:

    def __init__(self, dirpath, files, main_data_dict):
        self.dirpath = dirpath
        self.files = files
        self.main_data_dict = main_data_dict

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
            self.main_data_dict[row_data[0]] = round(volatility, 3)


def get_max_zero_tickers_volatility():
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
    calculation_of_volatility = [CalculationOfVolatility(dirpath=dirpath, files=files, main_data_dict=main_dict) for
                                 dirpath, dirname, files in os.walk(SOURCE_PATH)]

    for calc in calculation_of_volatility:
        calc.run()

    get_max_zero_tickers_volatility()


if __name__ == '__main__':
    main()
