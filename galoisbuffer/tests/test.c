#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#include "galois.h"

#define N 10000


int main(int argc, char* argv[])
{
    char *a, *b;
    int i, iters = 100000;
    
    a = (char*) malloc(sizeof(char)*N);
    b = (char*) malloc(sizeof(char)*N);

    srand(time(NULL));

    for(i=0; i<N; i++) {
        a[i] = rand();
        b[i] = rand();
    }

    for(i=0; i<iters; i++) {
        galois_region_xor(a, b, a, N);
        galois_w16_region_multiply(a, 23761, N, a, 0);
    }

    printf("%d\n", a[N-1]);
    return 0;
}
