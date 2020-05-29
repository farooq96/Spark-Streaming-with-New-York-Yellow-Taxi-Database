# Spark-Streaming-with-New-York-Yellow-Taxi-Database

Data Can be downloaded from here: https://lintool.github.io/bespin-data/taxi-data.tar.gz

To build the project:

mvn clean package

1. 
To execute:
/spark/bin/spark-submit --class ie.ucd.csl.comp47470.EventCount target/COMP47470-Project-3-1.0.0-fatjar.jar --input taxi-data --checkpoint checkpoint --output output

Output:
find output-* -name "part*" | xargs grep 'all' | sed -E 's/^output-([0-9]+)\/part-[0-9]+/\1/' | sort -n

2.
To execute:
/spark/bin/spark-submit --class ie.ucd.csl.comp47470.RegionEventCount target/COMP47470-Project-3-1.0.0-fatjar.jar --input taxi-data --checkpoint checkpoint --output output

Output:
find output-* -name "part*" | xargs grep 'goldman' | sed -E 's/^output-([0-9]+)\/part-[0-9]+/\1/' | sort -n

find output-* -name "part*" | xargs grep 'citigroup' | sed -E 's/^output-([0-9]+)\/part-[0-9]+/\1/' | sort -n

3.
To execute:
A)
/spark/bin/spark-submit --class ie.ucd.csl.comp47470.TrendingArrivals target/COMP47470-Project-3-1.0.0-fatjar.jar --input taxi-data --checkpoint checkpoint --output output&>output.log

output:

grep "Number of arrivals" output.log
