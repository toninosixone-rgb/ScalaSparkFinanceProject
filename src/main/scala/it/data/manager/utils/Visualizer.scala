package it.data.manager.utils

import org.apache.spark.sql.DataFrame
import java.io.{File, PrintWriter}
import java.awt.Desktop
import java.net.URI

object Visualizer {

  /**
   * Prende i dati elaborati e genera una pagina HTML con grafici interattivi.
   * Utilizza la libreria JavaScript Plotly (via CDN) per la visualizzazione.
   */
  def generateStockChart(df: DataFrame, ticker: String): Unit = {
    // 1. Convertiamo il DataFrame in una lista di mappe per estrarre i dati in Scala
    // Limitiamo a 500 righe per la visualizzazione per non appesantire il browser
    val data = df.filter(s"Ticker = '$ticker'")
      .orderBy("trading_day")
      .collect()

    val dates = data.map(r => s"'${r.getAs[java.sql.Date]("trading_day").toString}'").mkString(",")
    val closePrices = data.map(r => r.getAs[Double]("Close")).mkString(",")
    val sma50 = data.map(r => r.getAs[Double]("SMA_50")).mkString(",")
    val sma200 = data.map(r => r.getAs[Double]("SMA_200")).mkString(",")

    // 2. Creiamo il contenuto HTML/JavaScript
    val htmlContent = s"""
                         |<!DOCTYPE html>
                         |<html>
                         |<head>
                         |    <title>Analisi Finanziaria - $ticker</title>
                         |    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                         |    <style>
                         |        body { font-family: sans-serif; background-color: #f4f7f6; margin: 20px; }
                         |        .container { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
                         |        h1 { color: #2c3e50; }
                         |    </style>
                         |</head>
                         |<body>
                         |    <div class="container">
                         |        <h1>Dashboard Analitica: $ticker</h1>
                         |        <div id="chart" style="width:100%;height:600px;"></div>
                         |    </div>
                         |    <script>
                         |        var traceClose = {
                         |            x: [$dates], y: [$closePrices],
                         |            type: 'scatter', mode: 'lines', name: 'Prezzo Chiusura',
                         |            line: {color: '#17BECF', width: 2}
                         |        };
                         |        var traceSMA50 = {
                         |            x: [$dates], y: [$sma50],
                         |            type: 'scatter', mode: 'lines', name: 'SMA 50',
                         |            line: {color: '#FF7F0E', width: 1.5, dash: 'dot'}
                         |        };
                         |        var traceSMA200 = {
                         |            x: [$dates], y: [$sma200],
                         |            type: 'scatter', mode: 'lines', name: 'SMA 200',
                         |            line: {color: '#D62728', width: 1.5}
                         |        };
                         |        var data = [traceClose, traceSMA50, traceSMA200];
                         |        var layout = {
                         |            title: 'Prezzo Azionario e Medie Mobili ($ticker)',
                         |            xaxis: { title: 'Data', rangeslider: {visible: true} },
                         |            yaxis: { title: 'Prezzo (USD)', autoresize: true }
                         |        };
                         |        Plotly.newPlot('chart', data, layout);
                         |    </script>
                         |</body>
                         |</html>
    """.stripMargin

    // 3. Salviamo il file HTML e apriamolo nel browser
    val file = new File(s"target/dashboard_$ticker.html")
    val writer = new PrintWriter(file)
    writer.write(htmlContent)
    writer.close()

    println(s"SUCCESS: Dashboard generata in ${file.getAbsolutePath}")

    // Apre il browser automaticamente
    if (Desktop.isDesktopSupported && Desktop.getDesktop.isSupported(Desktop.Action.BROWSE)) {
      Desktop.getDesktop.browse(file.toURI)
    }
  }
}