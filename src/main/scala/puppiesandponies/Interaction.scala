package puppiesandponies

import org.joda.time.LocalDate

case class Interaction(user: Int, item: Int, timestamp: Long) {

  def isBefore(date: LocalDate): Boolean = {
    new LocalDate(timestamp).isBefore(date)
  }
}
