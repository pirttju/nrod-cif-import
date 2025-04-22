#!/bin/bash
set -e

# PostgreSQL settings (use .pgpass for authentication)
user="raiteilla"
database="raiteilla"

# Temporary file
savetofile="autoupdate.cif"

# Script starts
lastday=`psql -d $database -U $user -AXqtc "SELECT extract(isodow FROM date_of_extract) FROM nrod.header ORDER BY date_of_extract DESC LIMIT 1"`
nextday=$((lastday+1))

echo "last=${lastday} next=${nextday}"

if [[ $nextday -gt 7 ]]; then
    nextday=1
fi

rm ${savetofile}

CMD=(./getschedule.sh -o "${savetofile}.gz" -d ${nextday} -u)
"${CMD[@]}"

CMD=(python3 cifimport.py -d "${database}" -U "${user}" "${savetofile}")
"${CMD[@]}"
