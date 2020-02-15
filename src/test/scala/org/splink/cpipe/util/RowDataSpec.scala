package org.splink.cpipe.util

import java.time.Instant
import java.util.Date

/**
  * Copyright (C) 15.02.20 - REstore NV
  */
class RowDataSpec extends org.scalatest.FunSpec {
  //using defs so it is immutable
  def values = Array[AnyRef](
    new java.lang.Integer(3),
    new java.lang.Double(1.5),
    new java.lang.Long(10L),
    new java.lang.Short(1.toShort),
    new java.lang.Byte(2.toByte),
    new java.lang.Float(2.5),
    "String",
    new java.util.Date(10000)
  )

  def names = Array(
    "Integer",
    "Double",
    "Long",
    "Short",
    "Byte",
    "Float",
    "String",
    "Date",
  )

  def rowData = RowData(
    names,
    values
  )

  describe("RowData types") {
    it("should return scala types") {
      val rd = rowData
      import rd._
      assert(get("Integer").get == 3)
      assert(get("Double").get == 1.5)
      assert(get("Long").get == 10L)
      assert(get("Short").get == 1.toShort)
      assert(get("Byte").get == 2.toByte)
      assert(get("Float").get == 2.5f)
      assert(get("String").get == "String")
      assert(get("Date").get == Instant.ofEpochMilli(10000))

      assert(getValue(getIdx("Integer").get) == 3)
      assert(getValue(getIdx("Double").get) == 1.5)
      assert(getValue(getIdx("Long").get) == 10L)
      assert(getValue(getIdx("Short").get) == 1.toShort)
      assert(getValue(getIdx("Byte").get) == 2.toByte)
      assert(getValue(getIdx("Float").get) == 2.5f)
      assert(getValue(getIdx("String").get) == "String")
      assert(getValue(getIdx("Date").get) == Instant.ofEpochMilli(10000))

    }

    it("should have cassandra types internally") {
      val rd = rowData
      import rd._
      update("Integer", 10)
      update("Double", 15.5)
      update("Long", 100L)
      update("Short", 10.toShort)
      update("Byte", 20.toByte)
      update("Float", 20.5f)
      update("String", "String0")
      update("Date", Instant.ofEpochMilli(10002))
      val internal = rd.data
      internal.toSeq == Seq(
        10,
        15.5,
        100L,
        10.toShort,
        20.toByte,
        20.5f,
        "String0",
        new java.sql.Date(10002)
      )
    }
  }
}
