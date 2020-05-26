package com.spark.client

import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.services.sqs.AmazonSQSClientBuilder
import com.spark.conf.{AppProperties, BasicAWSCredentials}

//OBJECT TO CREATE A CONNECTION CLIENT INTO AWS SQS SERVICE
object SQSClient {

  val property: AppProperties = new AppProperties()

  def get() = {
    AmazonSQSClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(BasicAWSCredentials.get()))
      .withRegion(property.get("aws.region")).build()
  }

}
