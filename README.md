# BigData_Summer_2017
Small summer's course 2017


## 2017-08-23 Добавлено первое задание

Входные данные:
market-price.csv

Структура выходных данных:
TIME_KEY, формат yyyyMM
MIN_PRICE, с округлением до 2х знаков после запятой
MAX_PRICE, с округлением до 2х знаков после запятой

Задание:
Необходимо реализовать приложение с использованием парадигмы MapReduce, которое бы
выводило максимальную и минимальную цену за каждый месяц.

Технические требования:
Реализовать класс Mapper, Reducer. Плюсом будет реализация класса Driver.
В качестве сборщика проекта использовать maven.
Для работы с датами использовать библиотеку joda-time.
Для демонстрации работы и тестирования приложения использовать MRUnit tests.

Дополнительно:
Приложен класс TestUtils.java - для загрузки/записи данных.
Приложен класс TestExample.java - пример MRUnit теста.