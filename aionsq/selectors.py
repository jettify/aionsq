import abc
import random


class AbstractSelector(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def select(self, connections):
        pass  # pragma: no cover


class RandomSelector(AbstractSelector):

    def select(self, connections):
        return random.choice(connections)


class RoundRobinSelector(AbstractSelector):
    def __init__(self):
        self._current = 0

    def select(self, connections):
        self._current += 1
        if self._current >= len(connections):
            self._current = 0
        return connections[self._current]
