class FilterBase:
    def __init__(self, config):
        self.config = config

    def process(self):
        raise NotImplementedError("Subclasses should implement this.")