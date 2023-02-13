from itertools import product
from datetime import date

from sqlalchemy import create_engine, Integer, String, Date, func
from sqlalchemy.orm import sessionmaker, mapped_column, DeclarativeBase

from defaults import DEFAULT_DATABASE_URI, DEFAULT_F_TYPE, DEFAULT_Q_TYPE,\
    DEFAULT_DATES


class Base(DeclarativeBase):
    pass


class TableItem(Base):
    """
    Модель данных:
    id - первичный ключ,
    company - название компании,
    f_type - значение fact или forecast,
    q_type - значение Qliq или Qoil,
    date - дата,
    value - числовое значение.
    """
    __tablename__ = 'worktable'
    id = mapped_column(Integer, primary_key=True)
    company = mapped_column(String(30), nullable=False)
    f_type = mapped_column(String(10), nullable=False)
    q_type = mapped_column(String(10), nullable=False)
    date = mapped_column(Date)
    value = mapped_column(Integer)


class BaseConnector:
    """
    Класс, осуществляющий операции с БД. Дефолтная БД по умолчанию - sqlite.
    """
    def __init__(self, dbUrl=DEFAULT_DATABASE_URI):
        """
        Создание подключения к базе. Создаем движок, описываем таблицу и
        связываем ее с моделью.
        """
        self.engine = create_engine(dbUrl, echo=True)
        self.table = TableItem
        self.table().metadata.create_all(self.engine)
        self.session = sessionmaker(bind=self.engine)

    def make_load_session(self, data):
        """
        Создаем сессию, загружаем данные в базу и делаем коммит.
        После коммита удаляем сессию.
        """
        self.current_session = self.session()
        self.load_data(data)
        self.current_session.commit()
        del self.current_session

    def load_data(self, data):
        """
        Непосредственный разбор датафрейма, подготовка данных и создание на их
        основе таблицы
        """
        for column in data.itertuples(index=False):
            _, company, *values = column
            zipped = zip(list(product(DEFAULT_F_TYPE, DEFAULT_Q_TYPE,
                                      DEFAULT_DATES)), values)
            for item in zipped:
                (f_type, q_type, datemark), value = item
                unit = self.table(company=company, f_type=f_type,
                                  q_type=q_type, date=date(*datemark),
                                  value=value)
                self.current_session.add(unit)
        print('Грузим данные...')

    def get_totals(self):
        """
        Вычисляем суммы по датам по всем комбинациям показателей отдельно для
        каждой компании и общие. Возвращаем результат в виде списка кортежей.
        """
        self.current_session = self.session()
        t = self.table
        print("Обрабатываем данные...")
        s = self.current_session.query(
            func.sum(t.value), t.f_type, t.q_type, t.date
        ).group_by(
            t.f_type, t.q_type, t.date
        ).order_by(
            t.date, t.q_type, t.f_type
        )
        c = self.current_session.query(
            t.company, func.sum(t.value), t.f_type, t.q_type, t.date
        ).group_by(
            t.company, t.f_type, t.q_type, t.date
        ).order_by(
            t.date, t.q_type, t.f_type, t.company
        )
        res = list(c.all())
        addition = [('total', *i) for i in list(s.all())]
        del self.current_session
        for i, a in enumerate(addition):
            res.insert(i*3+2, a)
        print("Создаем отчет...")
        return res
