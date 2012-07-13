#instances="i-baa9c4ee i-a4a9c4f0 i-a6a9c4f2 i-a0a9c4f4 i-a2a9c4f6 i-aca9c4f8 i-aea9c4fa i-a8a9c4fc i-aaa9c4fe i-54a8c500 i-56a8c502 i-50a8c504 i-52a8c506 i-5ca8c508 i-5ea8c50a i-58a8c50c"
instances=" i-2a6f077e i-286f077c i-2e6f077a i-2c6f0778 i-226f0776 i-206f0774 i-266f0772 i-246f0770 i-3a6f076e i-386f076c i-3e6f076a i-3c6f0768 i-326f0766 i-306f0764 i-366f0762 i-346f0760"
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
