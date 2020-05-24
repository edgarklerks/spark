package org.apache.spark.sql.glue.util

import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.plans.logical.Histogram
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.Serialization.{write,read}

private[spark] object Stats {

  /**
   * Used to store the statistics
   */

  private val glueMetastoreStatisticsOption = s"spark.sql.glue.stats"
  private val glueMetastoreTableStatisticsOption = s"${glueMetastoreStatisticsOption}.table"
  private val glueMetastoreTableRowCountStatisticsOption = s"${glueMetastoreTableStatisticsOption}.row_count"
  private val glueMetastoreTableByteSizeStatisticsOption = s"${glueMetastoreTableStatisticsOption}.byte_size"

  private val glueMetastoreColumnsStatisticsOption = s"${glueMetastoreStatisticsOption}.columns"
  /**
   * Update the statistics in the parameters map
   * @param column column name the statistic belong to
   * @param statname the name of the statistic.
   * @param v the actual value, can be None, then the stat will be removed.
   * @param parameters The current parameters as Map
   * @return Update map
   */
  private def updateColumnStatToParameters(column: String)( statname : String,  v: Option[String], parameters : Map[String,String]) : Map[String,String] = {
    val storeKey = s"${glueMetastoreColumnsStatisticsOption}.${column}.${statname}"
    v.map(value => parameters + ((storeKey, value)))
      .getOrElse(parameters - storeKey)
  }

  /**
   * Stores the statitistics in human readable format per column and table.
   * @param parameters
   * @param stats
   * @return
   */
  def statsToParameters(parameters : Map[String,String], stats : Option[CatalogStatistics])  = {
    stats match {

      case None => parameters.filterKeys(k => !k.startsWith(glueMetastoreStatisticsOption))
      case Some(stats) => {
        val rowCount = stats.rowCount
        val byteSize = stats.sizeInBytes
        val tableStats = rowCount
          .map(rc => parameters + ((glueMetastoreTableRowCountStatisticsOption + ".row_count", rc.toString()))).getOrElse(parameters - (glueMetastoreTableRowCountStatisticsOption))
          .+((glueMetastoreTableByteSizeStatisticsOption,byteSize.toString()))

        stats.colStats.foldLeft(tableStats){case (parameters, (k,cstats)) =>
          val updater = updateColumnStatToParameters(k) _
          updater("version", Some(cstats.version.toString),
          updater("nullCount", cstats.nullCount.map(_.toString()),
          updater("histogram", cstats.histogram.map(hist => write[Histogram](hist)(DefaultFormats)),
          updater("distinctCount", cstats.distinctCount.map(_.toString),
          updater("avgLen", cstats.avgLen.map(_.toString),
          updater("min", cstats.min.map(_.toString),
          updater("max",cstats.max.map(_.toString),parameters)))))))
        }
      }
    }
  }

}
