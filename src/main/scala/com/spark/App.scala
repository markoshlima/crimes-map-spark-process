package com.spark

import java.io.BufferedReader
import java.io.InputStreamReader

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter

import com.spark.client.S3Client
import com.spark.model.{S3EventTriggerModel, StructCrimesModel}
import com.spark.service.SQSService
import org.apache.spark.sql.SparkSession

import scala.util.parsing.json.JSONObject

//spark-submit --class com.spark.App ./scala-maven-0.1-SNAPSHOT-jar-with-dependencies.jar

object App {
  def main(args: Array[String]) {

    val hadoopApi = "hdfs://localhost:9000"

    //DEFINE SPARK SESSION
    println("spark session")
    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

    //CREATE SQS CLIENT
    println("set sqs client")
    val sqsService = new SQSService()

    //READ MESSAGE AND PARSE S3 INFORMATIONS
    println("read messages from SQS and parse")
    val event: S3EventTriggerModel = sqsService.read()

    //HADOOP CONFIGURATION
    println("hadoop configuration")
    val conf = new Configuration()
    conf.set("fs.defaultFS", hadoopApi)
    val fs = FileSystem.get(conf)

    //DEFINE DESTINATION HDFS PATH
    println("destination hdfs")
    val finalPath = "/rawfiles/"+event.uuidFileName
    val output = fs.create(new Path(finalPath))

    //CREATE S3 CLIENT
    println("set s3 client")
    val s3Client = S3Client.get()

    //GET S3 FILE
    println("get s3 file")
    val obj = s3Client.getObject(event.bucketName, event.fileName)

    //SAVE FILE INTO HDFS
    println("write file")
    val reader = new BufferedReader(new InputStreamReader(obj.getObjectContent()))
    val writer = new PrintWriter(output)
    var line = reader.readLine()
    do{
      line = reader.readLine()
      writer.print(line)
      writer.print("\n")
    }while(line != null)
    writer.close()

    //REMOVE SQS
    println("delete sqs message")
    sqsService.delete(event.receiptHandle)

    //DEFINE SQL TEMPLATE
    println("sql template")
    val customSchema = StructCrimesModel.getStruct()

    //LOAD FILE FROM HDFS
    println("load file")
    val crimes = spark.read.schema(customSchema).option("encoding", "ISO-8859-1") .option("delimiter", ";").option("mode","DROPMALFORMED").format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").load(hadoopApi+finalPath).toDF()

    //CREATE VIEW
    println("create view")
    crimes.createOrReplaceTempView("crimes");

    //QUERY (MINING)
    println("sql file")
    val query = "SELECT DATAOCORRENCIA, HORAOCORRENCIA, PERIDOOCORRENCIA, LOGRADOURO, NUMERO, BAIRRO, CIDADE, UF, LATITUDE, LONGITUDE, RUBRICA, DESCR_MARCA_VEICULO, DESCR_COR_VEICULO, ANO_FABRICACAO FROM crimes WHERE upper(CIDADE) == upper('S.PAULO') AND (upper(DESCRICAOLOCAL) == upper('Via Pública') OR upper(DESCRICAOLOCAL) == upper('Estacionamento Público')) AND (upper(RUBRICA) == upper('Furto (art. 155) - VEICULO') OR upper(RUBRICA) == upper('Roubo (art. 157) - VEICULO')) AND LATITUDE is not null AND LONGITUDE is not null AND PERIDOOCORRENCIA != 'EM HORA INCERTA' AND DESCR_TIPO_VEICULO is not null AND DESCR_MARCA_VEICULO is not null"
    val sql = spark.sql(query);

    //REMOVE NULL ITENS
    println("treating null")
    val sqlfill = sql.na.fill("", Seq("DATAOCORRENCIA")).na.fill("", Seq("HORAOCORRENCIA")).na.fill("", Seq("PERIDOOCORRENCIA")).na.fill("", Seq("LOGRADOURO")).na.fill("", Seq("NUMERO")).na.fill("", Seq("BAIRRO")).na.fill("", Seq("UF")).na.fill("", Seq("DESCR_COR_VEICULO")).na.fill("", Seq("ANO_FABRICACAO"))

    //SHOW ITENS
    sqlfill.foreach {
      row => {
        val map = row.getValuesMap(row.schema.fieldNames)
        val output = JSONObject(map)
        println(output)
      }
    }

    println("Done!")

  }

}
