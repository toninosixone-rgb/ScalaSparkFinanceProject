package it.data.manager.config

import org.apache.spark.sql.SparkSession

/**
 * Wrapper per la SparkSession per centralizzare la configurazione.
 *
 * @param appName Nome dell'applicazione Spark.
 * @param master  Modo di esecuzione (es. "local[*]" o URL del cluster).
 */
case class SparkConfig(appName: String, master: String) {

  // Metodo per creare o recuperare la SparkSession
  def createSparkSession(): SparkSession = {
    SparkSession.builder
      .appName(appName)
      .master(master)
      // Configurazione cruciale per il supporto Delta Lake
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
  }

  // Puoi aggiungere qui altri metodi di configurazione globali,
  // come l'impostazione dei log level.
  def configureLogging(spark: SparkSession): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
  }
}
