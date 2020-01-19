AUTO_GEN_QUEUE_NAME = ""

EXCHANGE_TYPE_FANOUT = "fanout"


class Printable:
    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)
