AUTO_GEN_QUEUE_NAME = ""

DEFAULT_EXCHANGE = ""

EXCHANGE_TYPE_FANOUT = "fanout"


class Printable:
    """
    Makes the print format like I want it :-).
    """

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)
