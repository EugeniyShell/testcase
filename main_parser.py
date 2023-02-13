import os
import sys

import pandas

from base_class import BaseConnector
from defaults import DEFAULT_FILENAME


def get_file(filename: str) -> str:
    """
    Проверяем существование и формат (по расширению) файла перед загрузкой.
    """
    # проверяем существование файла
    if not os.path.isfile(filename):
        raise FileNotFoundError(f'Файла {filename} не существует.')
    # проверяем формат файла
    if filename.split('.')[-1] != 'xlsx':
        raise FileNotFoundError(f'Файл {filename} имеет неверный формат.')
    # возвращаем проверенное имя файла
    return filename


def read_file(file: str) -> pandas.DataFrame:
    """
    Загружаем данные из файла при помощи pandas, проверяем на соответствие
    нашему образцу и возвращаем в подготовленном виде.
    """
    # загружаем файл в pandas - здесь дополнительных проверок не требуется,
    # при неверном внутреннем формате pandas сам выдаст нужную ошибку
    excel = pandas.ExcelFile(file, engine='openpyxl')
    # проверяем на соответствие образцу количество листов Excel в файле
    if len(excel.sheet_names) > 1:
        raise pandas.errors.DataError("Too many data sheets in file.")
    # проверяем на соответствие образцу заголовки таблицы
    headers = pandas.read_excel(excel, header=None, nrows=3)
    if not check_header(headers):
        raise pandas.errors.DataError("Strange formatting table.")
    # возвращаем данные в виде pandas.DataFrame
    return pandas.read_excel(excel, header=None, skiprows=3)


def check_header(header: pandas.core.frame.DataFrame) -> bool:
    """
    Вынос во вспомогательную функцию проверки заголовка таблицы для улучшения
    читаемости.
    """
    try:
        return all([
            header[0][0] == 'id', header[1][0] == 'company', header[2][0] == 'fact', header[6][0] == 'forecast',
            header[2][1] == header[6][1] == 'Qliq', header[4][1] == header[8][1] == 'Qoil',
            header[2][2] == header[4][2] == header[6][2] == header[8][2],
            header[3][2] == header[5][2] == header[7][2] == header[9][2]]
        )
    except KeyError:
        return False


def make_report(source: list) -> None:
    """
    Берем список кортежей и записываем его в файл excel.
    Закомментирован более ресурсоемкий вариант.
    """
    # import collections
    # total = collections.OrderedDict([
    #     (source[0][0], []),
    #     (source[1][0], []),
    #     (source[2][0], [])
    #  ])
    # for i in range(0, len(source), 3):
    #     for x,y in enumerate(total.keys()):
    #         total[y].append(source[x+i][1])
    # headers = collections.OrderedDict([
    #     ('Date', ([source[0][-1]]*4 + [source[-1][-1]]*4)),
    #     ('Amount', ([*[source[0][2]]*2, *[source[-1][2]]*2]*2)),
    #     ('Type', ([source[0][3],source[-1][3]]*4)),
    #  ])
    # headers.update(total)
    # df = pandas.DataFrame(headers)
    total = {
        'Date': [],
        'Reality': [],
        'Type': [],
        source[0][0]: [],
        source[1][0]: [],
        'Total': [],
    }
    for i in range(0, len(source), 3):
        pack_data(source, i, total)
    df = pandas.DataFrame(total)
    # С этого места оба варианта совпадают.
    df.to_excel('./teams.xlsx', sheet_name='Total', index=False)


def pack_data(source, i, total) -> None:
    """
    Вынос во вспомогательную функцию тела цикла для улучшения читаемости.
    """
    total['Date'].append(source[i][4])
    total['Reality'].append(source[i][2])
    total['Type'].append(source[i][3])
    total[source[0][0]].append(source[i][1])
    total[source[1][0]].append(source[i+1][1])
    total['Total'].append(source[i+2][1])


def base_update(dataframe: pandas.DataFrame) -> None:
    """
    Создаем экземпляр класса для операций с БД.
    Вызываем его метод для загрузки данных в БД.
    """
    connector = BaseConnector()
    connector.make_load_session(dataframe)
    print('Данные загружены.')
    make_report(connector.get_totals())
    print('Отчет создан.')


def main():
    """
    Основной рабочий процесс.
    """
    try:
        if len(sys.argv) > 1:
            for filename in sys.argv[1:]:
                base_update(read_file(get_file(filename)))
        else:
            base_update(read_file(get_file(DEFAULT_FILENAME)))
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()
