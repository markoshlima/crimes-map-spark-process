package com.spark.client

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.spark.conf.BasicAWSCredentials

object SQSClient {

  def get() = {
    AmazonSQSClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(BasicAWSCredentials.get()))
      .withRegion("sa-east-1").build()
  }

}
