try:
    import dl
    dl_mod = True
except:
    dl_mod = False
    
import sys
import os.path

libc_options = ['/lib/libc.so.6', '/lib/i386-linux-gnu/libc.so.6']

def setprocname(name=None):
    if not dl_mod: return False
    
    if name==None:
        name = sys.argv[0].split('/')[-1]

    for libc_path in libc_options:
        if os.path.exists(libc_path):
            try:
                libc = dl.open(libc_path)
                libc.call('prctl', 15, name, 0, 0, 0)
                return True
            except:
                return False
    return False
