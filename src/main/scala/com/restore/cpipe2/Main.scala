package com.restore.cpipe2

import com.google.inject.Guice
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

/**
 * Copyright (C) 24/05/2017 - REstore NV
 */
object Main extends App with LazyLogging {

  logger.info("cpipe2 initializing...")

  val injector = Guice.createInjector(new MainModule)
  val config = injector.getInstance(classOf[Config])

  logger.info("cpipe2 starting...")

  // TODO Insert code here...

  logger.info("cpipe2 stopped.")

}
