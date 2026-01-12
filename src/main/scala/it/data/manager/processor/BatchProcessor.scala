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

  // Percorso Silver aggiornato (Path assoluto Windows)
  val SILVER_PATH = "file:///C:/Users/ANTO/Documents/progetti/ScalaSparkFinanceProject/data/silver"

  /**
   * Legge il file CSV/txt
   */
  def readCsv(spark: SparkSession, filePath: String, tickerName: String): DataFrame = {
    spark.read
      .schema(STOCK_SCHEMA)
      .option("header", "true")
      .option("sep", ",")
      .csv(filePath)
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

    val inputFile = "data/raw/Stocks/zion.us.txt"
    val tickerName = "ZION"

    try {
      // 1. Lettura dati raw
      val rawDf = readCsv(spark, inputFile, tickerName)
      println("--- Anteprima dei Dati Raw ---")
      rawDf.show(5)

      // 2. Pulizia (Fase Silver iniziale)
      val cleanDf = transformAndLoad(spark, rawDf)

      // 3. Arricchimento (Calcolo Medie Mobili)
      // Qui creiamo il vero dato "Silver" arricchito
      println("INFO: Calcolo delle medie mobili in corso...")
      val dataWithSma50 = MetricCalculator.calculateEma(cleanDf, 50, "Close")
      val finalData = MetricCalculator.calculateEma(dataWithSma50, 200, "Close")

      println("--- Anteprima Dati Finali (Silver) ---")
      finalData.show(5)

      // 4. Scrittura nel Layer Silver (Delta Lake)
      // Ora scriviamo il dato finale nel percorso specificato
      println(s"INFO: Scrittura dati in corso nel path: $SILVER_PATH")
      finalData.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("Ticker")
        .save(SILVER_PATH)

      println("SUCCESS: Dati salvati correttamente nel layer Silver.")

      // 5. Visualizzazione
      Visualizer.generateStockChart(finalData, tickerName)

    } catch {
      case e: Exception =>
        println(s"ERROR: Errore durante il processamento: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
      println("INFO: Spark Session chiusa.")
    }
  }
}