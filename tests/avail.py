from scipy import comb

def avail(p, n, k):
    q = 1-p
    return sum(comb(n,i)*(p**i)*(q**(n-i)) for i in xrange(k,n+1))

ps = [.8,.9,.99]
codes = [(3,1), (35,25)]

for p in ps:
    for code in codes:
        print avail(p, *code)
