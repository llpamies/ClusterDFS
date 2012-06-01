import itertools

def popcount(x):
    count = 0
    while x>0:
        x &= x-1
        count += 1
    return count

def hamdist(x, y):
    dist = 0
    val = x ^ y
    #Count the number of set bits
    while val:
        dist += 1
        val &= val - 1
    return dist

def binl(x, n):
    b = bin(x)[2:]
    return [0]*(n-len(b)) + map(int,b)

#a = [1<<i for i in xrange(8)]
#a.extend([15, 93, 107, 113, 147, 166, 189, 218])

n = 3
blocks = [1,2,3,4,5,7]
blocks.sort(key=lambda x: int(''.join(map(str,reversed(binl(x,n))))))

for b in reversed(blocks):
    print b, binl(b, n)

'''
for i in xrange(1):
    print blocks
    m = max((popcount(x&y),x,y) for x,y in itertools.combinations(blocks,2))

    print m

    r,x,y = m
    xadd = r^x
    yadd = r^y

    print r,xadd,'-->',x
    print r,yadd,'-->',y

    if r not in blocks:
        blocks.append(r)
    if xadd not in blocks:
        blocks.append(xadd)
    if yadd not in blocks:
        blocks.append(yadd)
    
    blocks.remove(x)
    blocks.remove(y)
'''
