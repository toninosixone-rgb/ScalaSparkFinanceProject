package it.data.manager.processor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import it.data.manager.config.SparkConfig
import org.apache.spark.sql.functions.{col, lit, to_date} // Importa la classe di configurazione

object BatchProcessor {

  // Definisci lo schema esplicito come costante
  val STOCK_SCHEMA: StructType = StructType(Array(
    StructField("Date", StringType, nullable = false),
    StructField("Open", DoubleType, nullable = true),
    StructField("High", DoubleType, nullable = true),
    StructField("Low", DoubleType, nullable = true),
    StructField("Close", DoubleType, nullable = true),
    StructField("Volume", LongType, nullable = true),
    StructField("OpenInt", LongType, nullable = true)
  ))

  /**
   * Legge il file CSV/txt
   */
  def readCsv(spark: SparkSession, filePath: String, tickerName: String): DataFrame = {
    spark.read
      .schema(STOCK_SCHEMA)      // Usa lo schema definito
      .option("header", "true")
      .option("sep", ",")
      .csv(filePath)
      // INIEZIONE DEL TICKER: Aggiunge la colonna Ticker con un valore fisso (metadato)
      .withColumn("Ticker", lit(tickerName))
  }

  //ETL
  def transformAndLoad(spark: SparkSession, rawDf: DataFrame): DataFrame = {
    import spark.implicits._

    // 1. Data Cleaning e Trasformazione
    val cleanDf = rawDf
      // Rimuove righe con valori nulli essenziali (es. chiusura)
      .na.drop(Seq("Close", "Date", "Ticker"))
      // Converte la colonna 'Date' al tipo Date (cruciale per Window Functions e partizionamento)
      .withColumn("trading_day", to_date(col("Date"), "yyyy-MM-dd"))
      // Rimuove la colonna 'OpenInt' se non usata, per snellire
      .drop("OpenInt", "Date")
      // Ordina il DataFrame. Le Window Functions dipendono dall'ordinamento!
      .orderBy("Ticker", "trading_day")
      .cache() // Caching per riutilizzo, specialmente prima dei calcoli costosi

    println(s"INFO: Pulizia completata. Righe: ${cleanDf.count()}")
    cleanDf

  }


  def main(args: Array[String]): Unit = {

    // 1. Inizializzazione della configurazione Spark
    val config = SparkConfig("HistoricalBatchProcessor", "local[*]")
    val spark = config.createSparkSession()

    // 2. Configura il logging e il cleanup
    config.configureLogging(spark)

    // 3. Esegue la lettura del CSV
    val rawDf = readCsv(spark, "data/raw/Stocks/zion.us.txt", "ZION")

    // --- Inizio Logica di Trasformazione (Fase 1) ---

    println("--- Anteprima dei Dati Storici ---")
    rawDf.show(5)

    // QUI AGGIUNGERAI LA LOGICA DI PULIZIA, ARRICCHIMENTO (EMA), E SCRITTURA IN DELTA LAKE
    val transformDf = transformAndLoad(spark, rawDf)
    println("--- Trasformazione dei Dati Storici ---")
    transformDf.show(5)

    // --- Fine Logica di Trasformazione ---

    spark.stop()
  }
}