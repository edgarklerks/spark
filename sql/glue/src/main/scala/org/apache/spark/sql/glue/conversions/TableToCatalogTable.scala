package org.apache.spark.sql.glue.conversions

import com.amazonaws.services.glue.model.Table
import org.apache.spark.sql.catalyst.catalog.CatalogTable

object TableToCatalogTable extends Conversion[Table, CatalogTable] {
  override def apply(x: Table): CatalogTable = ???
}
