#instances="i-baa9c4ee i-a4a9c4f0 i-a6a9c4f2 i-a0a9c4f4 i-a2a9c4f6 i-aca9c4f8 i-aea9c4fa i-a8a9c4fc i-aaa9c4fe i-54a8c500 i-56a8c502 i-50a8c504 i-52a8c506 i-5ca8c508 i-5ea8c50a i-58a8c50c"
instances="i-26bbd572 i-20bbd574 i-22bbd576 i-2cbbd578 i-2ebbd57a i-28bbd57c i-2abbd57e i-d4bbd580 i-d6bbd582 i-d0bbd584 i-d2bbd586 i-dcbbd588 i-debbd58a i-d8bbd58c i-dabbd58e i-c4bbd590"
rm amazon.pssh.ext
rm amazon.pssh.int
rm amazon.pssh.ips
for inst in $instances
do
    line=`ec2-describe-instances $inst | grep "INSTANCE"`
    echo $line | cut -d ' ' -f 4 >> amazon.pssh.ext
    echo $line | cut -d ' ' -f 5 >> amazon.pssh.int
    echo $line | cut -d ' ' -f 15 >> amazon.pssh.ips
done
