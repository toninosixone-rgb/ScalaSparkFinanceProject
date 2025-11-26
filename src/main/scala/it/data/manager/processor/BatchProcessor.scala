package it.data.manager.processor

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.types._
import it.data.manager.config.SparkConfig // Importa la classe di configurazione

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
   * Legge il file CSV e ritorna un DataFrame.
   */
  def readCsv(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .schema(STOCK_SCHEMA)      // Usa lo schema definito
      .option("header", "true")
      .option("sep", ",")
      .csv(filePath)
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

    // --- Fine Logica di Trasformazione ---

    spark.stop()
  }
}