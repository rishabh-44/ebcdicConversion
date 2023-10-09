package com.spark.EbcdicConversion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


@SpringBootApplication
public class EbcdicConversionApplication {
    public static String Convert(String strToConvert, String in, String out) {
        try {

            Charset charset_in = Charset.forName(out);
            Charset charset_out = Charset.forName(in);

            CharsetDecoder decoder = charset_out.newDecoder();
            CharsetEncoder encoder = charset_in.newEncoder();

            CharBuffer uCharBuffer = CharBuffer.wrap(strToConvert);

            ByteBuffer bbuf = encoder.encode(uCharBuffer);
            CharBuffer cbuf = decoder.decode(bbuf);

            String s = cbuf.toString();
            return s;

        } catch (CharacterCodingException e) {
            return "";
        }
    }

    public static void main(String[] args) throws IOException {
        Path fileName = Path.of("src/main/resources/ebcdic.dat");

        String str = Files.readString(fileName);
        System.out.println(str);

        String asciiString = Convert(str, "CP037", "ISO-8859-1");
        System.out.println(asciiString);

        byte[] arr = asciiString.getBytes();
        Files.write(Path.of("src/main/resources/ascii.txt"), arr);


        String path = "src/main/resources/Schema.json";
        byte[] b = Files.readAllBytes(Paths.get(path));
        String jsonString = new String(b);
        StructType schema = (StructType) StructType.fromJson(jsonString);

        SparkSession spark = SparkSession.builder()
                .appName("EBCDICtoASCIIConversion")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> actual = spark.read()
                .format("csv")
                .schema(schema)
                .load("src/main/resources/ascii.txt");
//				.option("delimeter",",")
//				.option("encoding", "ISO-8859-1")
        actual.show();
        actual.printSchema();


// To generate parquet file

//		actual.write().parquet("src/main/resources/convertedAscii.parquet");

//        To read that parquet
//        Dataset parquetDf = spark.read()
//        .parquet("src/main/resources/convertedAscii.parquet/part-00000-8d8a86e2-6117-4092-9437-6921e958c6eb-c000.snappy.parquet");
//        parquetDf.show();


//the file values should be separated by a comma
//        .format("csv")

//If the data is separated by a different delimeter then we can use this
//				.option("delimeter",",")

// This can also be used for validating fields,if you didn't give schema then the dataset column names will be like c1,c2,c3.
//                .schema(schema)

// To load the file in spark
//                .load("src/main/resources/asciifile.dat");

// To print the dataframe
//        dataset.show();

//To print the schema of the Dataframe
//        dataset.printSchema();
//        SpringApplication.run(EbcdicConversionApplication.class, args);
    }
}