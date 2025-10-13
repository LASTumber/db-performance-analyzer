import timeit
from functools import partial


class PerformanceAnalyzer:
    """
    Класс для измерения времени выполнения функций с использованием timeit.
    """

    def __init__(self, number=10, repeat=3):
        """
        :param number: Количество выполнений функции за один замер.
        :param repeat: Количество повторений замеров.
        """
        self.number = number
        self.repeat = repeat

    def measure_time(self, func, *args, **kwargs):
        """
        Измеряет время выполнения переданной функции 'func' с ее аргументами.

        :param func: Функция, время выполнения которой нужно измерить.
        :param args: Позиционные аргументы для функции.
        :param kwargs: Именованные аргументы для функции.
        :return: Минимальное среднее время выполнения в секундах.
        """
        # Создаем обертку, которая будет вызывать нашу функцию с нужными аргументами.
        # Это необходимо, потому что timeit.repeat может вызывать только функции без аргументов.
        wrapped_func = partial(func, *args, **kwargs)

        try:
            # Выполняем замеры
            execution_times = timeit.repeat(
                stmt=wrapped_func,
                number=self.number,
                repeat=self.repeat
            )

            # timeit.repeat возвращает список времен. Мы берем среднее время одного выполнения
            # из лучшей попытки. (total_time / number)
            min_time = min(execution_times) / self.number
            return min_time

        except Exception as e:
            print(f"Ошибка во время измерения производительности: {e}")
            return float('inf')  # Возвращаем бесконечность в случае ошибки