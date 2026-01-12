package it.data.manager.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType


object MetricCalculator {

  /**
   * Calcola la Media Mobile Semplice (SMA) su un DataFrame Spark, simulando
   * l'approccio di base per la Media Mobile Esponenziale (EMA) utilizzando
   * le Window Functions.
   *
   *
   * @param df Il DataFrame con i dati storici (deve contenere 'Close' e 'Ticker').
   * @param periods Il numero di periodi (giorni) per la media mobile (es. 50, 200).
   * @param columnName Il nome della colonna di input (es. "Close").
   * @return DataFrame arricchito con la colonna SMA calcolata.
   */

  def calculateEma(df: DataFrame, periods: Int, columnName: String): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._

    val emaColName = s"SMA_${periods}"

    // 1. Definisce la Window:
    //    - partitionBy("Ticker"): Calcola la media mobile separatamente per ogni titolo.
    //    - orderBy("trading_day"): Ordina le righe all'interno di ogni partizione per data.
    //    - rowsBetween(-periods + 1, 0): Definisce la finestra mobile: include la riga
    //      corrente (0) e le (periods - 1) righe precedenti.
    val windowSpec: WindowSpec = Window
      .partitionBy("Ticker")
      .orderBy("trading_day")
      .rowsBetween(-periods + 1, 0)

    // 2. Applica la Window Function
    val dfWithMetrics = df.withColumn(
      emaColName,
      // Calcola la media (avg) della colonna specificata all'interno della finestra definita
      avg(col(columnName)).over(windowSpec)
    )

    println(s"INFO: Calcolata SMA di ${periods} periodi per la colonna ${columnName}.")

    dfWithMetrics
  }
}