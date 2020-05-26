package com.spark.conf

import com.amazonaws.auth.BasicAWSCredentials

//SINGLETON OBJECT TO GET CREDENTIALS TO ACCESS AWS RESOURCES
object BasicAWSCredentials {

  val property: AppProperties = new AppProperties()
  private var credentials: BasicAWSCredentials = null

  def get(): BasicAWSCredentials = {
    if(credentials == null){
      credentials = new BasicAWSCredentials(property.get("aws.credentials.accessKey"), property.get("aws.credentials.secretKey"))
    }
    credentials
  }

}
