package com.restore.cpipe2

import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import org.mockito.Mockito._

import com.google.inject.Injector
import com.typesafe.config.Config

/**
 * Copyright (C) 24/05/2017 - REstore NV
 */
class MainSpec extends FlatSpec with Matchers with MockitoSugar {

  "main" should "start correctly" in {
    Main.main(Array())

    Main.config shouldBe a[Config]
    Main.injector shouldBe a[Injector]
  }

}
