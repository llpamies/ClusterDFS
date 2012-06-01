import math
import signal
import sys
import itertools
import random

def toBinary(n):
  r = []
  while (n > 0):
    r.append(n % 2)
    n = n / 2
  return r

def test(a, n):
  """
  test(a, n) -> bool Tests whether n is complex.

  Returns:
    - True, if n is complex.
    - False, if n is probably prime.
  """
  b = toBinary(n - 1)
  d = 1
  for i in xrange(len(b) - 1, -1, -1):
    x = d
    d = (d * d) % n
    if d == 1 and x != 1 and x != n - 1:
      return True # Complex
    if b[i] == 1:
      d = (d * a) % n
  if d != 1:
    return True # Complex
  return False # Prime

def MillerRabin(n, s = 50):
  """
    MillerRabin(n, s = 1000) -> bool Checks whether n is prime or not

    Returns:
      - True, if n is probably prime.
      - False, if n is complex.
  """
  for j in xrange(1, s + 1):
    a = random.randint(1, n - 1)
    if (test(a, n)):
      return False # n is complex
  return True # n is prime

def reqbits(v):
    return int(math.ceil(math.log(v)/math.log(2)))

def cost(x):
    return (x - (2**32))/(2.**32)

def signal_handler(signal, frame):
    print 'You pressed Ctrl+C!'
    print minimum_set
    sys.exit(0)

def popcount(x):
    count = 0
    while x>0:
        x &= x-1
        count += 1
    return count

signal.signal(signal.SIGINT, signal_handler)
init = (4294967311)

# FOUND: (4429180927L, (0, 12, 27, 28, 29, 30, 31), 0.03124904609285295)
'''
minimum_set = None
minimum_val = None
x = 2**33
y = 2**32
while y>0:
    v = x - y
    if MillerRabin(v, 20):
        c = popcount(y)
        if c<minimum_val or minimum_val==None:
            minimum_val = c
            minimum_set = (y, c)
            print y, c
    y -= 1
'''

i = 0
while not MillerRabin(2**32+i, 20):
    i += 1
print i
