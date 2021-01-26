package reports

import cats.effect.IO
import cats.implicits._
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.log4cats.Logger
import de.vandermeer.asciitable.AsciiTable

object AveragePrice30MostContacted {

  import datatypes._

  def report(stream: Stream[IO, Option[Contact]])(
    implicit logger : Logger[IO], cache: SignallingRef[IO, Map[Int, Listing]]
  ) = generate(stream).through(stream => join(stream)).through(stream => output(stream))

  /** Calculates the average price for the thirty percent most contacted listings.
   * By counting the occurences of each listingId in the contactsStream. Stored as Typle2 in the Seq.
   * The sequence is then sorted to extract the 30% most contacted listing.
   * @param listingStream : stream of Option[Contact]
   * @param logger : logging
   * @return A Stream of Seq Collection of the 30% most contacted listings.
   */
  def generate(stream: Stream[IO, Option[Contact]])(implicit logger : Logger[IO])= {
    stream.fold(Map.empty[Int, Int]) {
      (acc, contact) => contact match {
        case Some(c) => acc.updatedWith(c.listingId) {
          count => Some(count.fold(1){ exist => exist + 1 })
        }
        case None => acc
      }
    }.map{
      el =>
        val all = el.view.toSeq.sortBy( - _._2)
        all.take((all.size * 0.3).toInt)
    }
  }

  /** Extract the related price for each listing identified in the Seq[(listingId, count)].
   * @param thirtyContacts: A Stream of Seq(listingId, Count) containing the 30% most contacted listings..
   * @param logger : Logger for logging.
   * @param cache: SignalingRef Map[listingId, Listing].
   * @return the average price.
   */
  def join(thirtyContacts : Stream[IO, Seq[(Int, Int)]])
          (implicit  logger: Logger[IO],
           cache: SignallingRef[IO, Map[Int, Listing]]) = {

    thirtyContacts.evalMap { contactsSeq =>
      cache.get >>= { listingsMap =>
        val sum = contactsSeq.foldLeft(0) {
          (acc, el) => acc +  listingsMap(el._1).price
        }
        (sum / contactsSeq.size)
          .pure[IO]
      }
    }
  }

  def output(stream : Stream[IO, Int])(implicit logger : Logger[IO]) = {

    val at = new AsciiTable()

    at.addRule()
    at.addRow("Average price")
    stream.evalMap{  average =>
      at.addRule()
      at.addRow(("€" +average.toString + ",-"))
      at.addRule()
      logger.info(s"\n${at.render()}")
    }
  }
}
