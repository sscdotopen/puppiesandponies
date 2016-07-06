package puppiesandponies

import java.util.Properties

object Config {

  val properties = new Properties()

  try {
    properties.load(getClass.getClassLoader.getResourceAsStream("config.properties"))
  } catch {
    case _ =>
      System.err.println("Unable to load config, please create a file config.properties in the project root, " +
        "check out config.properties.template for that")
  }

  def datasetDir() = properties.getProperty("dataset-root-dir")
  def statsDir() = properties.getProperty("statistics-root-dir")
}
