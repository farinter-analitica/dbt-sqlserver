from dataclasses import dataclass
import timeit

class NormalClass:
    def __init__(self, x, y):
        self.x = x
        self.y = y

@dataclass(frozen=True)
class FrozenDataclass:
    x: int
    y: int

@dataclass
class Dataclass:
    x: int
    y: int

normal_class_time = timeit.timeit(lambda: NormalClass(1, 2), number=1000000)
frozen_dataclass_time = timeit.timeit(lambda: FrozenDataclass(1, 2), number=1000000)
dataclass_time = timeit.timeit(lambda: Dataclass(1, 2), number=1000000)

print(f"Normal class: {normal_class_time:.2f} sec")
print(f"Frozen dataclass: {frozen_dataclass_time:.2f} sec")
print(f"Dataclass: {dataclass_time:.2f} sec")