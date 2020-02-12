package org.splink.cpipe.util

import scala.reflect.runtime.universe._
//some helper class for people who whant to use filters this is high performance stuff, why just for fun!
case class RowData(names: Array[String], data: Array[AnyRef]) extends Iterable[(String, AnyRef)] {
  override def iterator: Iterator[(String, AnyRef)] = names.iterator.zip(data.iterator)

  def get(name: String): Option[AnyRef] = {
    var idx = 0
    while (idx < names.length) {
      if (names(idx) == name) {
        return Some(data(idx))
      }
      idx += 1
    }
    None
  }

  def contains(name: String)(check: AnyRef => Boolean) ={
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

  def update(name: String, value: AnyRef) = {
    this.getIdx(name).foreach{ idx =>
      data(idx) = value
    }
    this
  }

  def transform[T](name: String, change: T => AnyRef) = {
    this.getIdx(name).foreach { idx =>
      data(idx) match {
        case Some(value: T) => data(idx) = change(value)
        case None =>
      }
    }
    this
  }

  def get(idx: Int): Option[(String, Any)] = {
    if (idx < names.length) {
      Some(names(idx) -> data(idx))
    } else {
      None
    }
  }

  /**
    * super fast get only use this when you use an element of names
    */
  def getFastUnsafe(name: String): Option[Any] = {
    var idx = 0
    while (idx < names.length) {
      if (names(idx) eq name) {
        //break
        return Some(data(idx))
      }
      idx += 1
    }
    None
  }
}
