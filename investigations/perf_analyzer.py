import timeit
from functools import partial


class PerformanceAnalyzer:
    def __init__(self, number=10, repeat=3):
        self.number = number
        self.repeat = repeat

    def measure_time(self, func, *args, **kwargs):
        wrapped_func = partial(func, *args, **kwargs)

        try:
            execution_times = timeit.repeat(
                stmt=wrapped_func,
                number=self.number,
                repeat=self.repeat
            )
            min_time = min(execution_times) / self.number
            return min_time

        except Exception as e:
            print(f"Ошибка во время измерения производительности: {e}")
            return float('inf')

    def get_mean_time(self, func, *args, **kwargs):
        wrapped_func = partial(func, *args, **kwargs)
        try:
            execution_times = timeit.repeat(stmt=wrapped_func, number=self.number, repeat=self.repeat)
            mean_time = (sum(execution_times) / len(execution_times)) / self.number
            return mean_time
        except Exception as e:
            print(f"Ошибка во время измерения производительности: {e}")
            return float('inf')