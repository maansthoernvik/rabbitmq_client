class NotAThread:
    """
    Used to keep unittests single-threaded and avoid annoying wait-logic.
    Implements the needed Thread interface methods like start/join/etc.
    """
    def __init__(self, target=None, daemon=False):
        self.target = target
        self.alive = False
        self.daemon = daemon

    def start(self):
        self.alive = True
        self.target()

    def is_alive(self):
        return self.alive

    def join(self):
        pass
