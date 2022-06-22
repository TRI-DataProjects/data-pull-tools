import time
from abc import ABC, abstractmethod
from enum import Enum
from threading import Thread


class SpinnerStatus(Enum):
    SPINNING = 1
    STOPPING = 2
    STOPPED = 3

class APromptSpinner(ABC):
    def __init__(self):
        self.status = SpinnerStatus.STOPPED
        self.delay = 1
        self.prompt_message = None
        self.thread = None

    @abstractmethod
    def prompt(self):
        pass

    @abstractmethod
    def step(self):
        pass

    @abstractmethod
    def final(self):
        pass

    def spin(self):
        start_spin = True
        while self.status == SpinnerStatus.SPINNING:
            self.prompt()
            self.step()
            time.sleep(self.delay)
        return
    
    def start_spin(self, message):
        self.prompt_message = message
        self.status = SpinnerStatus.SPINNING
        self.thread = Thread(target=self.spin)
        self.thread.start()

    def stop_spin(self):
        self.status = SpinnerStatus.STOPPING
        self.thread.join()
        self.final()

class DotsSpinner(APromptSpinner):
    def __init__(self):
        super().__init__()
        self.cur_step = 0
        self.max_dots = 4

    def prompt(self):
        dots = ' .' * self.cur_step
        blanks = '  ' * (self.max_dots - self.cur_step - 1)
        print(f'\r{self.prompt_message}{dots}{blanks} ', end='')
        pass

    def step(self):
        self.cur_step = ((self.cur_step + 1) % (self.max_dots))

    def final(self):
        dots = ' .' * (self.max_dots - 1)
        print(f'\r{self.prompt_message}{dots} ', end='')
        self.cur_step = 0
        self.status = SpinnerStatus.STOPPED
