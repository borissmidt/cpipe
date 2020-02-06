package com.restore.cpipe2

import com.google.inject.AbstractModule
import com.typesafe.config.{Config, ConfigFactory}

/**
 * Copyright (C) 24/05/2017 - REstore NV
 */
class MainModule extends AbstractModule {
  
  override def configure(): Unit = {
    bind[Config](classOf[Config]).toInstance(ConfigFactory.load())
  }

}
