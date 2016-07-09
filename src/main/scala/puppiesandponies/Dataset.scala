package puppiesandponies

import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.GZIPInputStream

import scala.io.Source

trait Dataset {

  def name(): String

  def inTemporalOrder(): Boolean

  def interactions(): TraversableOnce[Interaction]

  def numUsers: Int = -1
  def numItems: Int = -1
}

object Datasets {
  val ALL = Array(LastfmBandListeningsDataset, DBLPCoAuthorDataset, StackoverflowFavoritesDataset,
    MillionSongsInteractionStream, OrkutInteractionStream, YahooMusicInteractionStream)
}

object LastfmBandListeningsDataset extends Dataset {

  override def name(): String = "lastfm"
  override def numUsers: Int = 992
  override def numItems: Int = 174077
  override def inTemporalOrder(): Boolean = true

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/lastfm_band/out.lastfm_band")
      .getLines
      .filter { line => !line.startsWith("%") }
      .map { line =>
        val tokens = line.split(" ")
        Interaction(tokens(0).toInt - 1, tokens(1).toInt - 1, tokens(3).toLong * 1000)
      }
      .toArray
      .sortBy { _.timestamp }
  }
}

object LogSynthInteractionStream extends Dataset {

  override def name(): String = "synth"

  override def inTemporalOrder(): Boolean = false

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/synth/generated.csv")
      .getLines
      .filter { line => !line.startsWith("x1") }
      .map { line =>
        val tokens = line.split(",")
        Interaction(tokens(0).toInt, tokens(1).toInt, 1L)
      }
  }
}

object DBLPCoAuthorDataset extends Dataset {

  override def name(): String = "dblp"
  override def numUsers: Int = 1314050
  override def numItems: Int = 1314050
  override def inTemporalOrder(): Boolean = true

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/dblp/out.dblp_coauthor")
      .getLines
      .filter { line => !line.startsWith("%") }
      .map { line =>
        val tokens = line.split(" ")
        Interaction(tokens(0).toInt - 1, tokens(1).toInt - 1, tokens(4).toLong * 1000)
      }
      .toArray
      .sortBy { _.timestamp }
  }
}

object StackoverflowFavoritesDataset extends Dataset {

  override def name(): String = "stackoverflow"
  override def numUsers = 545196
    override def numItems = 96678 + 2
  override def inTemporalOrder(): Boolean = true

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/stackoverflow/out.stackexchange-stackoverflow")
      .getLines
      .filter { line => !line.startsWith("%") }
      .map { line =>
      val tokens = line.split(" ")
      Interaction(tokens(0).toInt - 1, tokens(1).toInt - 1, tokens(3).toLong * 1000)
    }
  }
}

object MillionSongsInteractionStream extends Dataset {

  override def name(): String = "millionsongs"

  override def inTemporalOrder(): Boolean = false

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/millionsongs/interactions.tsv")
      .getLines
      .map { line =>
        val tokens = line.split("\t")
        Interaction(tokens(0).toInt, tokens(1).toInt, 1L)
      }
  }
}

object OrkutInteractionStream extends Dataset {

  override def name(): String = "orkut"

  override def inTemporalOrder(): Boolean = false

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/orkut/out.orkut-groupmemberships")
      .getLines
      .filter { line => !line.startsWith("%") }
      .map { line =>
        val tokens = line.split(" ")
        Interaction(tokens(0).toInt, tokens(1).toInt, 1L)
      }
  }
}

object YahooMusicInteractionStream extends Dataset {

  override def name(): String = "yahoomusic"

  override def inTemporalOrder(): Boolean = false

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromInputStream(new GZIPInputStream(new BufferedInputStream(
        new FileInputStream(s"${Config.datasetDir}/yahoo-music/songs.csv.gz"))))
      .getLines
      .map { line =>
        val tokens = line.split(",")
        Interaction(tokens(0).toInt, tokens(1).toInt, 1L)
      }
  }
}