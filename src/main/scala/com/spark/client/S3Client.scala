package com.spark.client

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.spark.conf.{AppProperties, BasicAWSCredentials}

//OBJECT TO CREATE A CONNECTION CLIENT INTO AWS S3 SERVICE
object S3Client {

  val property: AppProperties = new AppProperties()

  def get() = {
    AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(BasicAWSCredentials.get()))
      .withRegion(property.get("aws.region")).build()
  }

}
