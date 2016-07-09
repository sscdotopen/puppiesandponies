package puppiesandponies.analysis

import java.util.Random

import puppiesandponies.{Dataset, Interaction}

object CooccurrenceAnalysis {

  def cooccurrences(numItems: Int, A: HistoryMatrix): UpperTriangularCooccurrenceMatrix = {

    val start = System.currentTimeMillis

    val numUsers = A.numUsers
    val AtA = new UpperTriangularCooccurrenceMatrix(numItems, 11)

    var user = 0
    while (user < numUsers) {
      val userHistory = A.historyOfUser(user)

      val itemsA = userHistory.iterator

      while (itemsA.hasNext) {
        val itemA = itemsA.nextInt()

        val itemsB = userHistory.iterator
        while (itemsB.hasNext) {
          val itemB = itemsB.nextInt()
          if (itemA < itemB) {
            AtA.observeCooccurrence(itemA, itemB)
          }
        }
      }

      user += 1
    }

    val duration = System.currentTimeMillis - start
    println(s"Cooccurrence computation took ${duration}ms, 0-norm ~ ${AtA.zeroNorm / 1000000}M, 1-norm ~ ${AtA.oneNorm / 1000000}M")

    AtA
  }

  def toHistoryMatrix(dataset: Dataset): HistoryMatrix = {

    val A = new HistoryMatrix(dataset.numUsers, 5)

    var numInteractionsProcessed = 0

    dataset.interactions.foreach { interaction =>

      val item = interaction.item
      val user = interaction.user

      A.observeInteraction(user, item)

      numInteractionsProcessed += 1
      if (numInteractionsProcessed % 1000000 == 0) {
        println(s"processed ${numInteractionsProcessed / 1000000}M interactions")
      }
    }

    A
  }

  def toDownsampledHistoryMatrix(dataset: Dataset, kmax: Int, fmax: Int) = {

    val userCounts = Array.ofDim[Int](dataset.numUsers)
    val userNonCutCounts = Array.ofDim[Int](dataset.numUsers)
    val itemCounts = Array.ofDim[Int](dataset.numItems)

    val Acut = new HistoryMatrix(dataset.numUsers, 5)

    val random = new Random(0xdeadbeef)

    var numInteractionsProcessed = 0

    dataset.interactions.foreach { interaction =>

      val item = interaction.item
      val user = interaction.user

      userNonCutCounts(user) += 1

      if (itemCounts(item) < fmax) {
        if (userCounts(user) < kmax) {
          // unconditionally accept this interaction
          Acut.observeInteraction(user, item)
          userCounts(user) += 1
          itemCounts(item) += 1
        } else {
          // maxed out this user. We may have to push out another interaction
          val sample = random.nextInt(userNonCutCounts(user))
          val history = Acut.historyOfUser(user)

          if (sample < history.size && history.getInt(sample) != item) {

            val previousItem = history.getInt(sample)
            history.set(sample, item)

            userCounts(user) += 1
            itemCounts(item) += 1
            itemCounts(previousItem) -= 1
          }
        }
      }

      numInteractionsProcessed += 1
      if (numInteractionsProcessed % 1000000 == 0) {
        println(s"processed ${numInteractionsProcessed / 1000000}M interactions")
      }
    }

    Acut
  }

}
