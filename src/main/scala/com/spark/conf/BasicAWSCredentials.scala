package com.spark.conf

import com.amazonaws.auth.BasicAWSCredentials

object BasicAWSCredentials {

  private var credentials: BasicAWSCredentials = null

  def get(): BasicAWSCredentials = {
    if(credentials == null){
      credentials = new BasicAWSCredentials("","")
    }
    credentials
  }

}
