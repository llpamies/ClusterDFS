import logging

def ClassLogger(cls):
    cls.logger = logging.getLogger(cls.__name__)
    return cls

class Config(object):
    @classmethod
    def from_args(cls, args):
        c = cls()   
        for k, v in args.__dict__.iteritems():
            if v!=None: c.__dict__[k] = v
        return c.check()
    
    @classmethod
    def from_dict(cls, d):
        c = cls()
        for k, v in d.iteritems():
            if v!=None: c.__dict__[k] = v
        return c.check()

    def check(self):
        pass