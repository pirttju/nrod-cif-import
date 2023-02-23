#!/bin/bash
set -e

# Settings
user=
pass=
protocol="https"
hostname="publicdatafeeds.networkrail.co.uk"
endpoint="/ntrod/CifFileAuthenticate"

# Script starts
usage() {
    echo "Usage: ${0} [-c] [-j] [-i] [-d <day>] [-f] [-o <filename>] [-s <server>]" 1>&2;
    echo "File format:" 1>&2;
    echo "  -c for CIF File Format (Default), or" 1>&2;
    echo "  -j for JSON File Format" 1>&2;
    echo "Options:" 1>&2;
    echo "  -d Daily update for a day of week, e.g. for Monday use -d mon, or -d 1" 1>&2;
    echo "  -f Full snapshot (Default)" 1>&2;
    echo "  -o Override output filename" 1>&2;
    echo "  -s Specify server (Default: ${hostname})" 1>&2;
    echo "  -u Uncompress the file" 1>&2;
    exit 0;
}

while getopts ":cjid:fo:s:u" opts; do
    case "${opts}" in
        c)
            fileformat="cif"
            ;;
        j)
            fileformat="json"
            ;;
        i)
            information=true
            ;;
        d)
            fullsnapshot=false
            dailyupdate=${OPTARG}
            ;;
        f)
            fullsnapshot=true
            ;;
        o)
            outputfile=${OPTARG}
            ;;
        s)
            hostname=${OPTARG}
            ;;
        u)
            uncompress=true
            ;;
        h | *)
            usage
            ;;
    esac
done
shift $((OPTIND-1))

if [ -z "${fileformat}" ]; then
    fileformat="cif"
fi

if [ "$fullsnapshot" = false ]; then
    type="CIF_ALL_UPDATE_DAILY"
    case "${dailyupdate}" in
        1 | mon)
            day="toc-update-mon"
            ;;
        2 | tue)
            day="toc-update-tue"
            ;;
        3 | wed)
            day="toc-update-wed"
            ;;
        4 | thu)
            day="toc-update-thu"
            ;;
        5 | fri)
            day="toc-update-fri"
            ;;
        6 | sat)
            day="toc-update-sat"
            ;;
        7 | sun)
            day="toc-update-sun"
            ;;
    esac
else
    type="CIF_ALL_FULL_DAILY"
    day="toc-full"
fi

# Generate filename
if [ -z "$outputfile" ]; then
    savetofile="${day}.${fileformat}.gz"
else
    savetofile="${outputfile}"
fi

# Generate query
if [ "$fileformat" = "cif" ]; then
    day="${day}.CIF.gz"
fi

query="type=${type}&day=${day}"
url="${protocol}://${hostname}${endpoint}?${query}"

if [ "$information" = true ]; then
    # Check headers
    CMD=(curl -I -L -u "${user}:${pass}" -o /dev/null "${url}")
    "${CMD[@]}"
else
    # Download the file
    CMD=(curl -R -L -u "${user}:${pass}" -o "${savetofile}" "${url}")
    "${CMD[@]}"
fi

if [ "$uncompress" = true ]; then
    echo "Uncompressing..."
    CMD=(gunzip "${savetofile}")
    "${CMD[@]}"
fi
