package puppiesandponies.exploratory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import puppiesandponies._

import scala.collection.mutable

object ItemDegreeDistribution extends App {

  Array(MillionSongsInteractionStream).foreach { stream =>
    degreeDistribution(stream)
  }

  def degreeDistribution(stream: InteractionStream) {

    println(s"Computing degree distribution of stream ${stream.name}")

    var degreePerItem = mutable.Map[Int, Int]()

    stream.interactions().foreach { interaction =>
      val item = interaction.item
      val degree = degreePerItem.getOrElse(item, 0)
      degreePerItem.put(item, degree + 1)
    }

    val degrees = degreePerItem.values.toArray.sorted

    val file = s"${Config.statsDir}/${stream.name}_itemdegrees.tsv"

    Files.write(Paths.get(file), degrees.mkString("\n").getBytes(StandardCharsets.UTF_8))
  }
}
