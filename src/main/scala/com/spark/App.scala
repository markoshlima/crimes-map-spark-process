package com.spark

import java.io.BufferedReader
import java.io.InputStreamReader

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import java.io.PrintWriter

import com.spark.client.S3Client
import com.spark.conf.AppProperties
import com.spark.model.{S3EventTriggerModel, StructCrimesModel}
import com.spark.service.{HDFSService, SQSService}
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.util.parsing.json.JSONObject

//spark-submit --class com.spark.App ./scala-maven-0.1-SNAPSHOT-jar-with-dependencies.jar

object App {
  def main(args: Array[String]) {

    // APPLICATION PROPERTY CLASS
    val property: AppProperties = new AppProperties()

    //DEFINE SPARK SESSION
    println("configuring spark session")
    val sparkSession = SparkSession.builder().appName(property.get("spark.application.name")).master("local").getOrCreate()

    //CREATE SQS SERVICE
    println("setting sqs service")
    val sqsService = new SQSService()

    //READ MESSAGE AND PARSE S3 INFORMATIONS
    println("reading messages from SQS and parse")
    val event: S3EventTriggerModel = sqsService.read()

    //CREATE S3 CLIENT
    println("setting s3 client")
    val s3Client = S3Client.get()

    //GET S3 FILE
    println("getting s3 file")
    val obj = s3Client.getObject(event.bucketName, event.fileName)

    //HADOOP SERVICE
    println("setting hadoop service")
    val hdfsService = new HDFSService()

    //DEFINE DESTINATION HDFS PATH AND SAVE FILE INTO HDFS
    println("writing file")
    val finalPath = property.get("hdfs.folder.upload")+event.uuidFileName
    val reader = new BufferedReader(new InputStreamReader(obj.getObjectContent(), "ISO-8859-1"))
    hdfsService.writeStream(reader, hdfsService.createPath(finalPath))

    //REMOVE SQS
    println("deleting sqs message")
    sqsService.delete(event.receiptHandle)

    //DEFINE SQL TEMPLATE
    println("defining sql template")
    val customSchema = StructCrimesModel.getStruct()

    //LOAD FILE FROM HDFS
    println("loading file")
    //val finalPath = property.get("hdfs.folder.upload")+"82d440b452f7d37bbdef9a98792a557b-dados-bo-2019-1-furto.csv"
    val crimes = sparkSession.read.schema(customSchema).option("encoding", "UTF-8").option("delimiter", ";").option("mode","DROPMALFORMED").format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").load(property.get("hdfs.api")+finalPath).toDF()

    //CREATE VIEW
    println("creating view")
    crimes.createOrReplaceTempView("crimes");

    //LOAD SQL FILE WITH QUERY
    println("loading query file")
    val query = Source.fromFile(property.get("spark.sql.path.crimes")).mkString
    //val query = Source.fromFile("src/main/resources/sql/count.sql").mkString
    println(query)

    //QUERY (MINING)
    println("doing query")
    val sql = sparkSession.sql(query);

    //REMOVE NULL ITENS
    println("treating null")
    val sqlfill = sql.na.fill("", Seq("DATAOCORRENCIA")).na.fill("", Seq("HORAOCORRENCIA")).na.fill("", Seq("PERIDOOCORRENCIA")).na.fill("", Seq("LOGRADOURO")).na.fill("", Seq("NUMERO")).na.fill("", Seq("BAIRRO")).na.fill("", Seq("UF")).na.fill("", Seq("DESCR_COR_VEICULO")).na.fill("", Seq("ANO_FABRICACAO"))

    //SHOW ITENS
    println("printing results")

    sqlfill.foreach {
      row => {
        val map = row.getValuesMap(row.schema.fieldNames)
        val output = JSONObject(map)
        println(output)
      }
    }

    println("Done!")

    sparkSession.close()

  }

}
