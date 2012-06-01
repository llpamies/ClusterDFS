#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#define N 5000


int main(int argc, char* argv[])
{
    char *a, *b;
    int i, j, iters = 100000;
    unsigned long long tmp;
    unsigned long long p = 1;
    p<<=32;
    p+=15;
    
    a = (char*) malloc(sizeof(char)*N);
    b = (char*) malloc(sizeof(char)*N);

    srand(time(NULL));

    for(i=0; i<N; i++) {
        a[i] = rand();
        b[i] = rand();
    }

    for(i=0; i<iters; i++) {
        for(j=0; j<N; j++) {
            tmp = a[i];
            tmp += b[i];
            tmp *= 31231237;
            a[i] = (unsigned int)(tmp%p);
        }
    }

    printf("%d\n", a[N-1]);
    return 0;
}
