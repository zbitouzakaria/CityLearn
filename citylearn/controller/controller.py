from typing import Any, Iterable, List
import numpy as np
from citylearn.base import Environment

class Controller(Environment):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def actions(self) -> List[List[Any]]:
        return self.__actions

    @actions.setter
    def actions(self, actions: List[Any]):
        self.__actions[self.time_step] = actions

    def select_actions(self):
        raise NotImplementedError

    def next_time_step(self):
        super().next_time_step()
        self.__actions.append([])

    def reset(self):
        super().reset()
        self.__actions = [[]]

class Thermostat:
    def __init__(
        self, dry_bulb_temperature_set_point_schedule: Iterable[float], 
        minimum_dry_bulb_temperature_set_point: float = None, maximum_dry_bulb_temperature_set_point: float = None, 
        dry_bulb_temperature_throttle_range: float = 3.0
    ):
        self.dry_bulb_temperature_set_point_schedule = dry_bulb_temperature_set_point_schedule
        self.minimum_dry_bulb_temperature_set_point = minimum_dry_bulb_temperature_set_point
        self.maximum_dry_bulb_temperature_set_point = maximum_dry_bulb_temperature_set_point
        self.dry_bulb_temperature_throttle_range = dry_bulb_temperature_throttle_range

    @property
    def dry_bulb_temperature_set_point_schedule(self) -> Iterable[float]:
        return self.__dry_bulb_temperature_set_point_schedule

    @property
    def minimum_dry_bulb_temperature_set_point(self) -> float:
        return self.__minimum_dry_bulb_temperature_set_point

    @property
    def maximum_dry_bulb_temperature_set_point(self) -> float:
        return self.__maximum_dry_bulb_temperature_set_point

    @property
    def dry_bulb_temperature_throttle_range(self) -> float:
        return self.__dry_bulb_temperature_throttle_range

    @dry_bulb_temperature_set_point_schedule.setter
    def dry_bulb_temperature_set_point_schedule(self, dry_bulb_temperature_set_point_schedule: Iterable[float]):
        self.__dry_bulb_temperature_set_point_schedule = dry_bulb_temperature_set_point_schedule

    @minimum_dry_bulb_temperature_set_point.setter
    def minimum_dry_bulb_temperature_set_point(self, minimum_dry_bulb_temperature_set_point: float):
        schedule_minimum = np.nanmin(self.dry_bulb_temperature_set_point_schedule)

        if minimum_dry_bulb_temperature_set_point is None:
            self.__minimum_dry_bulb_temperature_set_point = schedule_minimum
        else:
            assert minimum_dry_bulb_temperature_set_point <= schedule_minimum, 'minimum_dry_bulb_temperature_set_point must be >= schedule minimum'
            self.__minimum_dry_bulb_temperature_set_point = minimum_dry_bulb_temperature_set_point
    
    @maximum_dry_bulb_temperature_set_point.setter
    def maximum_dry_bulb_temperature_set_point(self, maximum_dry_bulb_temperature_set_point: float):
        schedule_maximum = np.nanmax(self.dry_bulb_temperature_set_point_schedule)

        if maximum_dry_bulb_temperature_set_point is None:
            self.__maximum_dry_bulb_temperature_set_point = schedule_maximum
        else:
            assert maximum_dry_bulb_temperature_set_point >= schedule_maximum, 'maximum_dry_bulb_temperature_set_point must be >= schedule maximum'
            self.__maximum_dry_bulb_temperature_set_point = maximum_dry_bulb_temperature_set_point

    @dry_bulb_temperature_throttle_range.setter
    def dry_bulb_temperature_throttle_range(self, dry_bulb_temperature_throttle_range: float):
        assert dry_bulb_temperature_throttle_range >= 0, 'dry_bulb_temperature_throttle_range >= 0'
        self.__dry_bulb_temperature_throttle_range = dry_bulb_temperature_throttle_range