package it.data.manager.processor

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import it.data.manager.config.SparkConfig
import it.data.manager.utils.{MetricCalculator, Visualizer}
import org.apache.spark.sql.functions.{col, input_file_name, lit, regexp_extract, to_date, upper} // Importa la classe di configurazione

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
  def readCsv(spark: SparkSession, filePath: String): DataFrame = {
    val read = spark.read
      .schema(STOCK_SCHEMA)      // Usa lo schema definito
      .option("header", "true")
      .option("sep", ",")
      .csv(filePath)
      // 1. Aggiunge il percorso completo del file
      .withColumn("input_file", input_file_name())
      // 2. Estrae il nome (assume formato "nome.us.txt").
      // La regex prende tutto ciò che non è uno slash ([^/]+) prima di ".us.txt"
      .withColumn("Ticker", regexp_extract(col("input_file"), "([^/]+)\\.us\\.txt$", 1))

    read
      // 3. Converte in maiuscolo (es. "zion" -> "ZION")
      .withColumn("Ticker", upper(col("Ticker")))
      // 4. Rimuove la colonna temporanea col percorso intero
      .drop("input_file")
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
    val rawDf = readCsv(spark, "data/raw/Stocks/zion.us.txt")

    // --- Inizio Logica di Trasformazione (Fase 1) ---

    println("--- Anteprima dei Dati Storici ---")
    rawDf.show(5)

    // QUI AGGIUNGERAI LA LOGICA DI PULIZIA, ARRICCHIMENTO (EMA), E SCRITTURA IN DELTA LAKE
    val transformDf = transformAndLoad(spark, rawDf)


    // B. Calcolo delle metriche (SMA/EMA) -> Questo genera il vero "finalData"
    // Usiamo il MetricCalculator per aggiungere le colonne SMA_50 e SMA_200
    val dataWithSma50 = MetricCalculator.calculateEma(transformDf, 50, "Close")
    val finalData = MetricCalculator.calculateEma(dataWithSma50, 200, "Close")

    println("--- Anteprima Dati Finali con Medie Mobili ---")
    finalData.show(5)

    // C. Visualizzazione
    // Ora passiamo il finalData al Visualizer per aprire la finestra su Google/Browser
    Visualizer.generateStockChart(finalData, "ZION")

    // --- Fine Logica di Trasformazione ---

    spark.stop()
  }
}