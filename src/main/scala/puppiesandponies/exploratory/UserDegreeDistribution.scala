package puppiesandponies.exploratory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import puppiesandponies.{Config, Dataset, LogSynthInteractionStream}

import scala.collection.mutable

object UserDegreeDistribution extends App {

  Array(LogSynthInteractionStream).foreach { stream =>
    degreeDistribution(stream)
  }

  def degreeDistribution(stream: Dataset) {

    println(s"Computing degree distribution of stream ${stream.name}")

    var degreePerUser = mutable.Map[Int, Int]()

    stream.interactions().foreach { interaction =>
      val user = interaction.user
      val degree = degreePerUser.getOrElse(user, 0)
      degreePerUser.put(user, degree + 1)
    }

    val degrees = degreePerUser.values.toArray.sorted

    val file = s"${Config.statsDir}/${stream.name}_userdegrees.tsv"

    Files.write(Paths.get(file), degrees.mkString("\n").getBytes(StandardCharsets.UTF_8))
  }

}
