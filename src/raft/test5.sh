i="0"

while [ $i -lt 10 ]
do
echo "test"
go test -run 2B
i=$[$i+1]
done