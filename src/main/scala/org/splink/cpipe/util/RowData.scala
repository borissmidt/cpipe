package org.splink.cpipe.util

import java.sql.Date
//some helper class for people who whant to use filters this is high performance stuff, why just for fun!
case class RowData(names: Array[String], data: Array[AnyRef]) extends Iterable[(String, AnyRef)] {
  override def iterator: Iterator[(String, AnyRef)] = names.iterator.zip(data.iterator)

  def get(name: String): Option[Any] = {
    var idx = 0
    while (idx < names.length) {
      if (names(idx) == name) {
        return Some(getValue(idx))
      }
      idx += 1
    }
    None
  }

  def contains(name: String)(check: Any => Boolean) ={
    get(name).exists(check(_))
  }

  def remove(name: String) ={
    RowConversions.iteratorToTwinArray(this.filter(_._1 == name))
  }

  def getIdx(name: String): Option[Int] = {
    var idx = 0
    while (idx < names.length) {
      if (names(idx) == name) {
        return Some(idx)
      }
      idx += 1
    }
    None
  }

  def update(name: String, value: Any) = {
    this.getIdx(name).foreach{ idx =>
      set(idx, value)
    }
    this
  }

  def set(idx: Int, value:Any) = {
    data(idx) = RowData.scalaTypesToCassandra(value)
  }

  def transform[T](name: String, change: T => Any) = {
    this.getIdx(name).foreach { idx =>
      data(idx) match {
        case Some(value: T) => set(idx,change(value))
        case None =>
      }
    }
    this
  }

  def get(idx: Int): Option[(String, Any)] = {
    if (idx < names.length) {
      Some(names(idx) -> getValue(idx))
    } else {
      None
    }
  }

  def getValue(idx: Int): Any = {
    RowData.cassandraTypesToScala(data(idx))
  }

}

object RowData{
  def cassandraTypesToScala(x: AnyRef) = x match{
    case x: java.lang.Integer => x.intValue()
    case x: java.lang.Double => x.doubleValue()
    case x: java.lang.Long => x.longValue()
    case x: java.lang.Short => x.shortValue()
    case x: java.lang.Byte => x.byteValue()
    case x: java.lang.Float => x.floatValue()
    case x: String => x
    case x: java.util.Date => x.toInstant
//    case x: java.sql.Time =>
//    case x: java.sql.Timestamp =>
    case x => x
  }
  def scalaTypesToCassandra(x: Any): AnyRef = x match{
    case x: Int => Int.box(x)
    case x: Double => Double.box(x)
    case x: Long => Long.box(x)
    case x: Short => Short.box(x)
    case x: Byte => Byte.box(x)
    case x: Float => Float.box(x)
    case x: String => x
    case x: java.time.Instant => new java.sql.Date(x.getEpochSecond)
    case x: AnyRef => x
  }
}
