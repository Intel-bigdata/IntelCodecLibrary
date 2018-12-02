# ICL - Intel Codec Library for BigData 

ICL - Intel Codec Library for BigData provides compression and decompression library
for Apache Hadoop/Spark to make use of the acceleration hardware for
compression/decompression.

Big data analytics are commonly performed on large data sets that are moved
within a Hadoop/Spark cluster containing high-volume, industry-standard servers.
A significant amount of time and network bandwidth can be saved when the data
is compressed before it is passed between servers, as long as the compression/
decompression operations are efficient and require negligible CPU cycles.
This is possible with the hardware-based compression delivered by Intel
acceleration hardware such as FPGA/QAT etc, which is easy to integrate into 
existing systems and networks using the available Intel drivers and patches.

## Online Documentation

https://github.com/Intel-bigdata/IntelCodecLibrary

## Building Intel Codec Library for BigData

### 1. Building with Maven

This option assumes that you have installed maven in your build machine. Also assumed to have java installed and set JAVA_HOME

Run the following command for building IntelCompressionCodec.jar and libIntelCompressionCodec.so

```
 mvn clean install
```

Native code building will be skipped in Windows machine as Intel Compression Codec native code can not be build in Windows.

When you run the build in Linux os, native code will be build automatically when run the above command.

If you want native building to be skipped in linux os explicitly, then you need to mention -DskipNative

```
 mvn clean install -DskipNative
```

By default above commands will run the test cases as well. TO skip the test cases to run use the following command

```
 mvn clean install -DskipTests
```

To run the specific test cases

```
 mvn clean test -Dtest=TestIntelCompressorDecompressor
```

## How to use Intel Codec Library for BigData 

### For Spark shuffle compression codec

Put below configurations to _$SPARK_HOME/conf/spark-defaults.conf_
```
spark.io.compression.codec com.intel.compression.spark.IntelCompressionCodec
spark.io.compression.codec.intel.codec lz4-ipp/zlib-ipp/igzip/zstd
spark.executor.extraClassPath      /path/to/IntelCompressionCodec-version.jar
spark.driver.extraClassPath        /path/to/IntelCompressionCodec-version.jar
```

#### For any security concerns, please visit https://01.org/security.

