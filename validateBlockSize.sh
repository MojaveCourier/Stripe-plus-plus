# find . -type f -name "0_D00" -exec ls -lh {} \;
# find . -type f -name "0_D01" -exec ls -lh {} \;
# find . -type f -name "0_D02" -exec ls -lh {} \;
# find . -type f -name "0_D03" -exec ls -lh {} \;
# find . -type f -name "0_G00" -exec ls -lh {} \;
# find . -type f -name "0_G01" -exec ls -lh {} \;
# find . -type f -name "0_G02" -exec ls -lh {} \;
# find . -type f -name "0_G03" -exec ls -lh {} \;
# find . -type f -name "0_L00" -exec ls -lh {} \;
# find . -type f -name "0_L01" -exec ls -lh {} \;

# find . -type f -name "1_D00" -exec ls -lh {} \;
# find . -type f -name "1_D01" -exec ls -lh {} \;
# find . -type f -name "1_D02" -exec ls -lh {} \;
# find . -type f -name "1_D03" -exec ls -lh {} \;
# find . -type f -name "1_G00" -exec ls -lh {} \;
# find . -type f -name "1_G01" -exec ls -lh {} \;
# find . -type f -name "1_G02" -exec ls -lh {} \;
# find . -type f -name "1_G03" -exec ls -lh {} \;
# find . -type f -name "1_L00" -exec ls -lh {} \;
# find . -type f -name "1_L01" -exec ls -lh {} \;

stripe_num=15

# # azure lrc
# k=30
# r=6
# z=6

# # optimal lrc
# k=30
# r=10
# z=2

# # uniform lrc
# k=30
# r=7
# z=5

k=30
r=6
z=6

for stripe_id in $(seq 0 $((stripe_num - 1)))
do
    for i in $(seq 0 $((k - 1)))
    do
        formatted_num=$(printf "%02d" $i)
        find . -type f -name "${stripe_id}_D${formatted_num}" -exec ls -lh {} \;
    done

    for i in $(seq 0 $((r - 1)))
    do
        formatted_num=$(printf "%02d" $i)
        find . -type f -name "${stripe_id}_G${formatted_num}" -exec ls -lh {} \;
    done

    for i in $(seq 0 $((z - 1)))
    do
        formatted_num=$(printf "%02d" $i)
        find . -type f -name "${stripe_id}_L${formatted_num}" -exec ls -lh {} \;
    done
done