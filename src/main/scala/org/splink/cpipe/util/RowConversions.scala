package org.splink.cpipe.util

import com.datastax.driver.core.Row

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object RowConversions {

  def rowToTwinArray(row: Row) = {
    val definitions = row.getColumnDefinitions.asScala
    val names = new Array[String](definitions.size)
    val data = new Array[AnyRef](definitions.size)
    definitions.zipWithIndex.foreach {
      case (definition, index) =>
        val column = row.getObject(definition.getName)
        names(index) = definition.getName
        data(index) = column
    }
    RowData(names, data)
  }

  def iteratorToTwinArray(row: Iterable[(String, AnyRef)]) = {
    val names = mutable.ArrayBuilder.make[String]
    val values = mutable.ArrayBuilder.make[AnyRef]
    row.foreach{case (name, value) =>
      names.addOne(name)
      values.addOne(value)
    }
    RowData(names.result(), values.result())
  }




}
