/*GNU GENERAL PUBLIC LICENSE
      Version 2, June 1991
{TFG-LourdesFernándezNieto}
Copyright (C) {2016}  {Lourdes Fernández Nieto}
*/
import java.io.File
import com.google.gson.Gson
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext._
import AccesoTwitter._
import scala.collection.mutable.HashMap
import java.io.FileWriter

/**
 * BaseDatos va a realizar un acceso a twitter a traves de las claves publicas y privadas dadas por la plataforma Twitter
 * Application Manager (https://apps.twitter.com/) para descargar un conjuto de tweets que se esten subiendo a la plataforma 
 * Twitter en el momento de la ejecucion de la aplicacion
 *
 */
 object BaseDatos {

  private var numTweetsCollected = 0L
  private var partNum = 0
  private var gson = new Gson()

  def main(args: Array[String]) {

    val checkpointDir = AccesoTwitter.getCheckpointDirectory()

    val apiKey = "XXXXXXXXXXXXXXXXXXXX"
    val apiSecret = "XXXXXXXXXXXXXXXXXXXX"
    val accessToken = "XXXXXXXXXXXXXXXXXXXX"
    val accessTokenSecret = "XXXXXXXXXXXXXXXXXXXX"
    AccesoTwitter.configureTwitterCredentials(apiKey, apiSecret, accessToken, accessTokenSecret)

    /** Para probar esta clase de torma aislada se realiza esta comprobacion
      System.err.println("Usage: " + this.getClass.getSimpleName + "<directorioSalida> <palabraFiltro>")
      System.exit(1)
    }*/
    
    val Array(directorioSalida, palabraFiltro)=args

    val outputDir = new File(directorioSalida.toString)

    outputDir.mkdirs()

    println("Inicializando Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))  

    val filter = Array(palabraFiltro)

    val tweetStream = TwitterUtils.createStream(ssc, None, filter).map(gson.toJson(_))

    tweetStream.foreachRDD( (rdd, time) => {
      val writer = new FileWriter(new File(directorioSalida + "/index.txt"),true)

      try {

        var printing =""
        var flag = false
        for(i <- rdd.toArray){

          if(!flag){
            flag = true
          }
          printing = i + "\n"
        }

        if(flag){
          printing = printing.substring(0,printing.length())
        }
        writer.write(printing)
      } finally{
       writer.close()
     }
     })
    
    ssc.start()
    ssc.awaitTermination()
  }
}
