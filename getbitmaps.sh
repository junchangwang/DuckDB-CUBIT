dic="CUBIT-dataset"

if [ -e ./CUBIT-dataset ]; then
	echo "cubit-dataset already exists"
else
	git clone https://github.com/junchangwang/CUBIT-dataset.git
	echo "download dataset sucessfully"
fi
bitmaps=(
	"discount"
	"linestatus"
	"o_orderkey"
	"orderkey"
	"partkey"
	"quantity"
	"receiptdate"
	"returnflag"
	"shipdate"
	"shipinstruct"
	"shipmode"
	"suppkey"
)
for file in ${bitmaps[@]}
do
	if [ -e ./bm_15000000_$file ]; then
		echo "bitmap $file already exists"
	else
		if [ -e ./CUBIT-dataset/DuckDB/10sf_${file}_bitmap.tar.gz ]; then
			tar -xf ./CUBIT-dataset/DuckDB/10sf_${file}_bitmap.tar.gz
		else
			cat ./CUBIT-dataset/DuckDB/10sf_${file}_bitmap/10sf_${file}_bitmap.tar.gz.parta* | tar -xzf -	
		fi
		echo "uncompress bitmap $file successfully"
	fi
done

