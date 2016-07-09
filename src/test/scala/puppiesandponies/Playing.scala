package puppiesandponies

import puppiesandponies.analysis.CooccurrenceAnalysis

object Playing extends App {

  val dataset = StackoverflowFavoritesDataset

  val Acut = CooccurrenceAnalysis.toDownsampledHistoryMatrix(dataset, 500, 500)
  val AtAcut = CooccurrenceAnalysis.cooccurrences(dataset.numItems, Acut)

  val A = CooccurrenceAnalysis.toHistoryMatrix(dataset)
  val AtA = CooccurrenceAnalysis.cooccurrences(dataset.numItems, A)


}
