import logging

def ClassLogger(cls):
    cls.logger = property(lambda self: logging.getLogger(cls.__name__))
    return cls
