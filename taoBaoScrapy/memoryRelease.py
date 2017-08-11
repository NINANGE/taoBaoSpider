#*_ coding:utf-8 _*_
import gc
import sys

class CGcLeak(object):
    def __init__(self):
        self._text = '#' * 10

    def __del__(self):
        pass

def make_circle_ref():
    _gcleak = CGcLeak()
    _gcleak._self = _gcleak     #自己循环引用自己
    print "_gcleak ref count0: %d" %(sys.getrefcount(_gcleak))
    del _gcleak
    try:
        print "_gcleak ref count1 :%d" %(sys.getrefcount(_gcleak))
    except UnboundLocalError:
        print "_gcleak is invalid!"

def memoryReleases():
    gc.enable()
    gc.set_debug(gc.DEBUG_COLLECTABLE | gc.DEBUG_UNCOLLECTABLE | gc.DEBUG_INSTANCES | gc.DEBUG_OBJECTS)

    print "begin leak test..."
    make_circle_ref()

    print "begin collect..."
    _unreachable = gc.collect()
    print "unreachable object num:%d" %(_unreachable)
    print "garbage object num:%d" %(len(gc.garbage))

# if __name__ == "__main__":
#     test_gcleak()