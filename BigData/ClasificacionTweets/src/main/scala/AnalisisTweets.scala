/**GNU GENERAL PUBLIC LICENSE
      Version 2, June 1991
{TFG-LourdesFernándezNieto}
Copyright (C) {2016}  {Lourdes Fernández Nieto}
*/

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.google.gson.{GsonBuilder, JsonParser}
import org.apache.spark.sql.SQLContext
import java.io.File
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.io._
import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.JavaConversions._

/**
 * AnalisisTweets va a ser la clase encargada de analizar los tweets recogidos con la clase BaseDatos.scala
 * Leeremos los tweets almacenados en el directorio indicado al ejecutar BaseDatos
 * para realizar un analisis de ellos a traves de consultas SQL
 */
 object AnalisisTweets {

    val jsonParser = new JsonParser()
    val gson = new GsonBuilder().setPrettyPrinting().create() 

    def main(args: Array[String]) {

    /** Para probar esta clase de torma aislada se realiza esta comprobacion
    if (args.length < 2) {
      System.err.println("Usage: " + this.getClass.getSimpleName + "<directorioEntrada> <directorioResultados> " )
      System.exit(1)
    }
    */
    val Array(directorioEntrada, directorioResultados) = args

    val outputDir = new File(directorioResultados.toString)
    
    /**Verificacion de existencia del directrio para prueba aislada
    if (outputDir.exists()) {
      System.err.println("ERROR - %s ya existe. Elimine o especifique otro directorio para guardar los resultados"
        .format(directorioResultados))
      System.exit(1)
    }
    */
    outputDir.mkdirs() 

    println("Inicializando Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val tablaTweets = sqlContext.jsonFile(directorioEntrada).cache()
    tablaTweets.registerTempTable("tablaTweets")

//-----------------------------------------------------------------------------------------------------------------------------
// A1)Analisis de los idiomas mas usados. Realizamos un filtro con los 15 idiomas mas usados del momento
//-----------------------------------------------------------------------------------------------------------------------------
println("A1 ------ NUMERO TOTAL DE TWEETS CLASIFICADO POR IDIOMA CON ORDEN DESCENDENTE ------")
val idiomas = sqlContext.sql("""
    SELECT 
    user.lang,
    COUNT(*) as cnt
    FROM tablaTweets 
    GROUP BY user.lang
    ORDER BY cnt DESC 
    LIMIT 15""")

val guardamos1 = idiomas.map(row =>"{" + """"""" +  "idioma" + """"""" + ":" + """"""" + row(0) + """"""" + "," + """"""" + "cantidad"+ """""""+ ":" + """""""+ row(1) + """"""" + "}" + ",")
guardamos1.coalesce(1,true).saveAsTextFile(directorioResultados + "/Idiomas")
guardamos1.collect.foreach(println)

println("----------------FIN DE A1, CLASIFICACION POR IDIOMA----------------")


//-----------------------------------------------------------------------------------------------------------------------------
// A2)Analiza en que parte del mundo, separado por zonas temporales, se estan escribiendo mas tweets en el momento.
//-----------------------------------------------------------------------------------------------------------------------------
println("A2 ------ ZONAS TEMPORALES CON MAYOR ACTIVIDAD EN TWITTER -------")
val zonas = sqlContext.sql("""
    SELECT
    user.timeZone,
    SUBSTR(createdAt, 0, 9),
    COUNT(*) AS total_count
    FROM tablaTweets 
    WHERE user.timeZone IS NOT NULL
    GROUP BY
    user.timeZone,
    SUBSTR(createdAt, 0, 9)
    ORDER BY total_count DESC
    LIMIT 15 """)

val guardamos2 = zonas.map(row =>"{" + """"""" +  "lugar" + """"""" + ":" + """"""" + row(0) + """"""" + "," + """"""" + "hora"+ """""""+ ":" + """""""+ row(1) + """"""" + "," + """"""" + "cantidad"+ """""""+ ":" + """""""+ row(2) + """""""+ "}" + ",")
guardamos2.collect.foreach(println)
guardamos2.coalesce(1,true).saveAsTextFile(directorioResultados + "/Lugares")

println("----------------FIN DE A2, ZONAS TEMPORALES CON MAYOR ACTIVIDAD EN TWITTER----------------")

//-----------------------------------------------------------------------------------------------------------------------------
// A3) ¿Quién es más influyente en función de número de favoritos?
//-----------------------------------------------------------------------------------------------------------------------------
println("A3 ------ DE LOS TWEETS RECOGIDOS, QUIEN TIENE MAYOR INFLUENCIA CON FAVORITOS ---")
val favoritos = sqlContext.sql("""
    SELECT
    tweet.favourites_screen_name,
    tweet.timezone,
    sum(favourites) AS total_favourites,
    count(*) AS favourites_count
    FROM (SELECT
        user.screenName as favourites_screen_name,
        text,
        user.timeZone as timezone,
        max(user.favouritesCount) as favourites
        FROM tablaTweets WHERE text <> ''
        GROUP BY 
        user.screenName,
        user.timeZone,
        text) tweet
GROUP BY tweet.favourites_screen_name, tweet.timezone
ORDER BY total_favourites DESC
LIMIT 10 """)
val guardamos3 = favoritos.map(row =>"{" + """"""" +  "identificador" + """"""" + ":" + """"""" + row(0) + """"""" 
    + "," + """"""" + "lugar"+ """""""+ ":" + """""""+ row(1) + """"""" 
    + "," + """"""" + "numero"+ """""""+ ":" + """""""+ row(2) + """""""+ "}" + ",")
guardamos3.collect.foreach(println)
guardamos3.coalesce(1,true).saveAsTextFile(directorioResultados + "/InfluenciaFavoritos")

println("----------------FIN DE A3, INFLUENCIA DE LOS TWEETS----------------")
//-----------------------------------------------------------------------------------------------------------------------------
// A4) ¿Quién es más influyente en función del número de seguidores?
//-----------------------------------------------------------------------------------------------------------------------------
println("A4 ------ DE LOS TWEETS RECOGIDOS, QUIEN TIENE MAYOR NUMERO DE SEGUIDORES ---")
val seguidores = sqlContext.sql("""
    SELECT
    tweet.followers_screen_name,
    tweet.timezone,
    sum(followers) AS total_followers,
    count(*) AS followers_count
    FROM (SELECT
        user.screenName as followers_screen_name,
        text,
        user.timeZone as timezone,
        max(user.followersCount) as followers
        FROM tablaTweets WHERE text <> ''
        GROUP BY 
        user.screenName, 
        user.timeZone,
        text) tweet
GROUP BY tweet.followers_screen_name, tweet.timezone
ORDER BY total_followers DESC
LIMIT 10 """)
val guardamos4 = seguidores.map(row =>"{" + """"""" +  "identificador" + """"""" + ":" + """"""" + row(0) + """"""" 
    + "," + """"""" + "lugar"+ """""""+ ":" + """""""+ row(1) + """"""" 
    + "," + """"""" + "numero"+ """""""+ ":" + """""""+ row(2) + """""""+ "}" + ",")
guardamos4.collect.foreach(println)
guardamos4.coalesce(1,true).saveAsTextFile(directorioResultados + "/InfluenciaSeguidores")

println("----------------FIN DE A4, INFLUENCIA DE LOS TWEETS----------------")
//-----------------------------------------------------------------------------------------------------------------------------
// A5) ¿Cuales son los #Hashtag del momento?. Limitamos a 10 los hashtags recogidos
//-----------------------------------------------------------------------------------------------------------------------------
println("A5 ------ DE LOS TWEETS RECOGIDOS, CUALES SON LOS HASHTAGS ---")
val etiquetas = sqlContext.sql("""
    SELECT
    hashtagEntities[0].text,
    count(*) AS total_count
    FROM tablaTweets
    WHERE hashtagEntities[0].text IS NOT NULL
    GROUP BY hashtagEntities[0].text
    ORDER BY total_count DESC
    LIMIT 10 """)
val guardamos5 = etiquetas.map(row =>"{" + """"""" +  "hashtag" + """"""" + ":" + """"""" + row(0) + """"""" + "," + """"""" + "cantidad"+ """""""+ ":" + """""""+ row(1) + """"""" + "}" + ",")
guardamos5.collect.foreach(println)
guardamos5.coalesce(1,true).saveAsTextFile(directorioResultados + "/Hashtags")

println("----------------FIN DE A5, HASHTAGS----------------")
}
}






