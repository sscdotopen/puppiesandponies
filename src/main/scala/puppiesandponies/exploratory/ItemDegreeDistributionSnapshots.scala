package puppiesandponies.exploratory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import puppiesandponies._


object ItemDegreeDistributionSnapshots extends App {

  val stream = StackoverflowInteractionStream
  val interactions = stream.interactions.toArray

  storeDegreeDistribution(interactions, 100000, stream.name())
  storeDegreeDistribution(interactions, 250000, stream.name())
  storeDegreeDistribution(interactions, 500000, stream.name())
  storeDegreeDistribution(interactions, 1000000, stream.name())
  
  /*val stream = Movielens10MInteractionStream
  val interactions = stream.interactions.toArray

  storeDegreeDistribution(interactions, 1000000, stream.name())
  storeDegreeDistribution(interactions, 5000000, stream.name())
  storeDegreeDistribution(interactions, 10000000, stream.name())*/

  /*val stream = LastfmInteractionStream
  val interactions = stream.interactions.toArray

  storeDegreeDistribution(interactions, 5000000, stream.name())
  storeDegreeDistribution(interactions, 10000000, stream.name())
  storeDegreeDistribution(interactions, 15000000, stream.name())
  storeDegreeDistribution(interactions, 19000000, stream.name())*/


  def storeDegreeDistribution(interactions: Array[Interaction], numInteractions: Int, dataset: String) {
    println(s"Computing degree distribution for ${numInteractions} interactions of ${dataset}")
    val snapshot = interactions.take(numInteractions)
    val degrees = snapshot
      .groupBy { _.item }
      .map { case (item, interactions) => interactions.size }
      .toArray
      .sorted

    degrees.foreach { println }

    val file = s"${Config.statsDir}/${dataset}_itemdegrees_at${numInteractions}.tsv"

    Files.write(Paths.get(file), degrees.mkString("\n").getBytes(StandardCharsets.UTF_8))
  }
}
