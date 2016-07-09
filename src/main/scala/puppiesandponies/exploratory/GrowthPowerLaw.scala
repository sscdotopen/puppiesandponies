package puppiesandponies.exploratory

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.joda.time.{LocalDate, Months, Years}
import puppiesandponies._

object GrowthPowerLaw extends App {

  val stream = DBLPCoAuthorDataset//StackoverflowInteractionStream//LastfmInteractionStream//StackoverflowInteractionStream

  assert(stream.inTemporalOrder())

  val interactions = stream.interactions().toArray

  println(interactions.size)

  val firstInteraction = interactions.minBy { _.timestamp }
  val lastInteraction = interactions.maxBy { _.timestamp }

  println(firstInteraction.timestamp)
  println(lastInteraction.timestamp)

  val youngest = new LocalDate(1980, 1, 1)//new LocalDate(firstInteraction.timestamp)
  val oldest = new LocalDate(lastInteraction.timestamp)

  println(s"${stream.name}: ${youngest} to ${oldest}")

  //val numMonths = Months.monthsBetween(youngest, oldest).getMonths
  val years = Years.yearsBetween(youngest, oldest).getYears
  //val startingMonth = youngest.withDayOfMonth(1)
  val startingYear = youngest.withDayOfYear(1)

  //val lines = for (additionalMonths <- 1 to numMonths) yield {
  val lines = for (additionalYears <- 1 to years) yield {

    //val cutOff = startingMonth.plusMonths(additionalMonths)
    val cutOff = startingYear.plusYears(additionalYears)
    val evolvedDataset = interactions.filter { _.isBefore(cutOff) }

    val numInteractions = evolvedDataset.size
    val numItems = evolvedDataset.map { _.item }.distinct.size

    //println(s"${additionalMonths}\t${numItems}\t${numInteractions}")
    println(s"${additionalYears}\t${numItems}\t${numInteractions}")
    //s"${additionalMonths}\t${numItems}\t${numInteractions}"
    s"${additionalYears}\t${numItems}\t${numInteractions}"

  }

  val file = s"${Config.statsDir}/${stream.name}_growth.tsv"

  Files.write(Paths.get(file), lines.mkString("\n").getBytes(StandardCharsets.UTF_8))

}
