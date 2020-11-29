"""
Exchange properties
"""


EXCHANGE_TYPE_FANOUT = "fanout"


"""
Generic class info
"""


class Printable:
    """
    Makes the print format like I want it :-).
    """

    def __str__(self):
        return "{} {}".format(self.__class__, self.__dict__)
