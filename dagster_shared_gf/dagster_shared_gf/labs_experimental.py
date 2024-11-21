from functools import wraps
import functools
def max_dt_with_lag_last_value_func(event: None = None, lag_days: int =1):
    return lag_days*2



class ComparableFunction:
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __call__(self, *args, **kwargs):
        return self.func(*self.args, *args, **{**self.kwargs, **kwargs})

    def __eq__(self, other):
        if isinstance(other, ComparableFunction):
            return self.func == other.func
        return self.func == other

    def __getattr__(self, name):
        return getattr(self.func, name)

def make_comparable(func, *args, **kwargs):
    return ComparableFunction(func, *args, **kwargs)


new_function = make_comparable(max_dt_with_lag_last_value_func, lag_days=2)

print(new_function.__name__)  # Output: max_dt_with_lag_last_value_func

print(max_dt_with_lag_last_value_func())

print(new_function())

assert new_function == max_dt_with_lag_last_value_func