#! /usr/bin/env zsh

RED='\033[0;31m'
NC='\033[0m' # No Color

if [ $# -lt 2 ] ; then 
 echo "${RED}USAGE:${NC} 
 sh $0 c1ed9bb97fc6ab89b0bc9998231e62f5 testname1 
 sh $0 c1ed9bb97fc6ab89b0bc9998231e62f5 testname1 8090" 
 echo " para1 is sspid,para2 is file name save in Documents" 
 exit 1; 
fi 
DATE=`date +%Y-%m-%d:%H:%M:%S`
SSPID=$1
FILE=$2
HOST=localhost
PORT=8080
if [ $# -gt 2 ];then
   PORT=$3
fi

if [ $# -gt 3 ];then
   HOST=$4
   echo "The host is: ${HOST}"
fi
echo "========command======================="
set -x
#curl -X GET http://${HOST}:${PORT}/v1/atv/ssp-order-rankers\?ssp_order_id\=${SSPID} -H "Authorization: bearer 616f5b025447f82dda83bfdc15215817" | python -m json.tool  > ~/Documents/${FILE}.json
set +x
echo "======================================"
echo ""
echo "API write to ~/Documents/${FILE}.json "
echo ""
echo ""
echo ""


echo "========command======================="
set -x 
curl -X POST http://${HOST}:${PORT}/v1/atv/ssp-orders/${SSPID}/submit\?send\=false -H "Authorization: bearer 647891920aecc192746989035a4246f5" > ~/Documents/${FILE}.xls
set +x
echo ""
echo ""
echo "======================================"
echo "download xls to ~/Documents/${FILE}.xls"

