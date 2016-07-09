package puppiesandponies.exploratory.cutcounter

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import puppiesandponies.Config

import scala.io.Source

object ConvertDistFiles extends App {

  convert("/home/ssc/Entwicklung/projects/puppiesandponies/cutounter-item.dist.csv", "cutcounter_itemdegrees200.tsv")
  convert("/home/ssc/Entwicklung/projects/puppiesandponies/cutounter-user.dist.csv", "cutcounter_userdegrees200.tsv")

  def convert(degreesFile: String, outFile: String) = {

    val degrees = Source.fromFile(degreesFile)
      .getLines
      .filter {
        !_.startsWith("interactions")
      }
      .flatMap { line =>

        val tokens = line.split(",")
        val degree = tokens(0).toInt
        val howOften = tokens(1).toInt

        Array.fill(howOften)(degree)
      }
      .toArray
      .sorted

    println(degrees.size)

    val file = s"${Config.statsDir}/${outFile}"

    Files.write(Paths.get(file), degrees.mkString("\n").getBytes(StandardCharsets.UTF_8))
  }
}
