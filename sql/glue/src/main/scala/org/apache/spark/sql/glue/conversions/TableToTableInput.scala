package org.apache.spark.sql.glue.conversions

import com.amazonaws.services.glue.model.{Table, TableInput}

object TableToTableInput extends Conversion[Table,TableInput] {
  /**
   * Table and TableInput contain the same information, but of course there is no method to convert between them.
   * Amazon seems to generate their SDK's from their rest API, which explains a couple of things (like the several redefinitions of the
   * idea of a Tag throughout the aws sdk).
   * @param table
   * @return
   */
  override def apply(table: Table): TableInput =
    new TableInput()
      .withDescription(table.getDescription)
      .withLastAccessTime(table.getLastAccessTime)
      .withLastAnalyzedTime(table.getLastAnalyzedTime)
      .withName(table.getName)
      .withOwner(table.getOwner)
      .withParameters(table.getParameters)
      .withPartitionKeys(table.getPartitionKeys)
      .withRetention(table.getRetention)
      .withStorageDescriptor(table.getStorageDescriptor)
      .withTableType(table.getTableType)
      .withViewExpandedText(table.getViewExpandedText)
      .withViewOriginalText(table.getViewOriginalText)
}
