package com.test.com.customer

import org.joda.time.format.DateTimeFormat

object CIConstants {
  val DELIMITER="\\|"
  val format= new java.text.SimpleDateFormat("MM-dd-YYYY")
  val formatter = DateTimeFormat.forPattern("MM/dd/YYYY HH:mm:ss");
  val PRODUCT_PATH="C:/Users/dhana/OneDrive/Desktop/scala_doc_dataflair/data/Product.txt"
  val SALES_PATH="C:/Users/dhana/OneDrive/Desktop/scala_doc_dataflair/data/Sales.txt"
  val REFUND_PATH="C:/Users/dhana/OneDrive/Desktop/scala_doc_dataflair/data/Refund.txt"
  val CUSTOMER_PATH="C:/Users/dhana/OneDrive/Desktop/scala_doc_dataflair/data/Customer.txt"
  val BASE_OUTPUT_DIR="C:/Users/dhana/OneDrive/Desktop/spark_output/"
}
