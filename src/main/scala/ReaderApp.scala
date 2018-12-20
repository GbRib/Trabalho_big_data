package mba

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.streaming.OutputMode._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._

 
object ReaderApp {

  def main(args: Array[String]){
    if (args.length < 1) {
      System.err.println("Usage: ReaderApp diretorio")
      System.exit(1)
    }

     
    val diretorio : String = args(0)

    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("ReaderApp")
      .getOrCreate()

    import spark.implicits._
     
    val esquema = new StructType()
      	.add("id","int")
    	.add("timestamp","timestamp")
    	.add("tipoconsulta","int")
    	.add("mercado","string")
    	.add("bairro","string")
    	.add("sexo","string")
    	.add("idade","int")
        .add("preco","float")

    val leituras = spark.readStream
		.schema(esquema)
    	.csv(diretorio)
       
   
       val atividades = leituras.as[Leitura]
    
  
    val contagens = atividades.withWatermark("timestamp", "10 minutes").groupBy("timestamp","mercado","tipoconsulta")
    	.count
    	.withColumnRenamed("count","leituras")
    
    val query = contagens.writeStream
   
      .trigger(Trigger.ProcessingTime(5.seconds))
      .format("csv")
      .option("path","/home/gabriel/streaming/resultado")
      .option("checkpointLocation","/home/gabriel/streaming/resultado_checkpoint")
      .outputMode(Append)
      .start()
      
	query.awaitTermination()
  }
}
