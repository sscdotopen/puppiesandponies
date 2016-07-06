package puppiesandponies

import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.GZIPInputStream

import scala.io.Source

trait InteractionStream {

  def name(): String

  def inTemporalOrder(): Boolean

  def interactions(): TraversableOnce[Interaction]
}

object InteractionStreams {
  val ALL_STREAMS = Array(Movielens1MInteractionStream, Movielens10MInteractionStream, LastfmInteractionStream,
    DBLPInteractionStream, StackoverflowInteractionStream, MillionSongsInteractionStream, OrkutInteractionStream,
    YahooMusicInteractionStream)
}

object Movielens1MInteractionStream extends InteractionStream {

  override def name(): String = "movielens1M"

  override def inTemporalOrder(): Boolean = true

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/movielens1M/ratings.dat")
      .getLines
      .map { line =>
        val tokens = line.split("::")
        Interaction(tokens(0).toInt, tokens(1).toInt, tokens(3).toLong * 1000)
      }
      .toArray
      .sortBy { _.timestamp }
  }
}

object Movielens10MInteractionStream extends InteractionStream {

  override def name(): String = "movielens10M"

  override def inTemporalOrder(): Boolean = true

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/movielens-10m_rating/out.movielens-10m_rating")
      .getLines
      .filter { line => !line.startsWith("%") }
      .map { line =>
      val tokens = line.split(" ")
      Interaction(tokens(0).toInt, tokens(1).toInt, tokens(3).toLong * 1000)
    }
      .toArray
      .sortBy { _.timestamp }
  }
}

object LastfmInteractionStream extends InteractionStream {

  override def name(): String = "lastfm"

  override def inTemporalOrder(): Boolean = true

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/lastfm_band/out.lastfm_band")
      .getLines
      .filter { line => !line.startsWith("%") }
      .map { line =>
        val tokens = line.split(" ")
        Interaction(tokens(0).toInt, tokens(1).toInt, tokens(3).toLong * 1000)
      }
      .toArray
      .sortBy { _.timestamp }
  }
}

object DBLPInteractionStream extends InteractionStream {

  override def name(): String = "dblp"

  override def inTemporalOrder(): Boolean = true

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/tedandme/dblp/out.dblp_coauthor")
      .getLines
      .filter { line => !line.startsWith("%") }
      .map { line =>
        val tokens = line.split(" ")
        Interaction(tokens(0).toInt, tokens(1).toInt, tokens(4).toLong * 1000)
      }
      .toArray
      .sortBy { _.timestamp }
  }
}

object StackoverflowInteractionStream extends InteractionStream {

  override def name(): String = "stackoverflow"

  override def inTemporalOrder(): Boolean = true

  override def interactions(): TraversableOnce[Interaction] = {
    Source
      .fromFile(s"${Config.datasetDir}/stackoverflow/out.stackexchange-stackoverflow")
      .getLines
      .filter { line => !line.startsWith("%") }
      .map { line =>
      val tokens = line.split(" ")
      Interaction(tokens(0).toInt, tokens(1).toInt, tokens(3).toLong * 1000)
    }
  }
}

object MillionSongsInteractionStream extends InteractionStream {

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

object OrkutInteractionStream extends InteractionStream {

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

object YahooMusicInteractionStream extends InteractionStream {

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