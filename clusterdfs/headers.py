
class DataNodeHeader(object):
    OP_STORE = 0
    OP_RETRIEVE = 1
    OP_REMOVE = 2
    OP_CODING = 3
    
class NameNodeHeader:
    OP_PING = 0
    OP_GETNODES = 1
