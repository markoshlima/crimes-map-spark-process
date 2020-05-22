package com.spark.client

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.spark.conf.BasicAWSCredentials

object S3Client {

  def get() = {
    AmazonS3ClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(BasicAWSCredentials.get()))
      .withRegion("sa-east-1").build()
  }

}
