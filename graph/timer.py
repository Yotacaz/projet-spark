from typing import Any, Callable, TypeVar
import time
from functools import wraps

# Timing configuration - set to False to disable timing logs
ENABLE_TIMING = True

T = TypeVar("T")


def timed(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to measure and log the execution time of a function."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        if not ENABLE_TIMING:
            return func(*args, **kwargs)
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        print_color = "\033[34m"  if elapsed < 1 else "\033[33m" if elapsed < 5 else "\033[31m"
        print(f"{print_color}[TIMING] {func.__name__} took {elapsed:.4f} seconds\033[0m")
        return result

    return wrapper


class Timer:
    """Context manager for timing code blocks."""

    def __init__(self, name: str = "block"):
        self.name = name

    def __enter__(self) -> "Timer":
        if ENABLE_TIMING:
            self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        if ENABLE_TIMING:
            elapsed = time.perf_counter() - self.start_time
            print_color = "\033[34m"  if elapsed < 1 else "\033[33m" if elapsed < 5 else "\033[31m"
            print(f"{print_color}[TIMING] {self.name} took {elapsed:.4f} seconds\033[0m")
