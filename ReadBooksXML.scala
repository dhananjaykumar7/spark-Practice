package com.test.com.customer
import com.test.{Books, BooksDiscounted}
import org.apache.spark.sql.{Encoders, SparkSession}
object ReadBooksXML {
  def main(args :Array[String]): Unit ={
    System.setProperty("hadoop.home.dir", "C:\\Hadoop\\hadoop-3.1.2")
    System.setProperty("spark.sql.warehouse.dir", "file:\\C:\\Users\\dhana\\OneDrive\\Desktop\\spark-2.4.3-bin-hadoop2.7\\spark-warehouse")

    val spark = SparkSession.builder.master("local[1]").appName("ReadBooksXml").getOrCreate()

    import spark.implicits._
    val ds = spark.sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "book")
      .load("C:\\Users\\dhana\\OneDrive\\Desktop\\scala_doc_dataflair\\data\\Books.xml").as[Books]

    val newds= ds.map(f =>{BooksDiscounted(f._id,f.author,f.description,f.price,f.publish_date,f.title, f.price - f.price*20/100)})
    newds.printSchema()
    newds.show()

    newds.foreach(f=>{
      println("price :"+f.price + ", Discounted Price :"+f.discountPrice)
    })
    //First element
    println("First Element" +newds.first()._id)

  }

}
