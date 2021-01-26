package reports


import cats.effect.IO
import cats.implicits._
import de.vandermeer.asciitable.AsciiTable
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.log4cats.Logger

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.MapView

object TopFive {

  import datatypes._

  /** Formats the date from to String as : MM.YYYY
   * @param date : the date represented with long.
   * @return the formated date.
   */
  def formatDate(date: Long): String = {
    val d : Date = new Date(date)
    new SimpleDateFormat("MM.yyyy").format(d)
  }

  def report(stream: Stream[IO, Option[Contact]])(
    implicit logger: Logger[IO], cache: SignallingRef[IO, Map[Int, Listing]]
  ) = generate(stream).through(stream => join(stream)).through(stream => output(stream))

  /** Groups the Contacts by month. and counts the occurences for each listingId.
   * It constructs a Stream of collection of (Month, (idlisting, count)).
   * @param stream: the stream of Option[Contact]
   * @param logger for logging
   * @return a Stream of Map[Month, (idlisting, count)]
   */
  def generate(stream: Stream[IO, Option[Contact]])(
    implicit logger: Logger[IO]) = {

    stream.fold(Map.empty[String, Map[Int, Int]]) { (acc, contact) =>
      contact.map{ contact =>
        acc.updatedWith(formatDate(contact.contactDate)) {
          existingMonth => Some(existingMonth.fold(Map(contact.listingId -> 1)) {
            previous => previous.updatedWith(contact.listingId) {
              count => Some(count.fold(1) { c => c + 1 })
            }
          })
        }
      }.getOrElse(acc)
    }
  }

  /** Uses the top five most contacted listings per month. to extract the detailed listing info.
   * @param streamContactInfo : Stream of Map[Month, (idlisting, count)]
   * @param logger for logging.
   * @param cache: SignalingRef of Map[ListingId, Listing] representing the listings details info.
   * @return A stream of a Map  of (Month, List[(Listings, contactRatecount)]
   */
  def join(streamContactInfo: Stream[IO, Map[String, Map[Int, Int]]])(
    implicit logger: Logger[IO], cache: SignallingRef[IO, Map[Int, Listing]]
  ) : Stream[IO, MapView[String, List[(Listing, Int)]]]  =
    streamContactInfo.evalMap{ contactMap =>
      cache.get >>= { listingsMap =>
        contactMap.view.mapValues(_.toList.sortBy(-_._2).take(5).map {
          case (listingId, count) => listingsMap(listingId) -> count
        }).pure[IO]
      }
    }

  def output(stream: Stream[IO, MapView[String, List[(Listing, Int)]]])(implicit logger: Logger[IO]) =
    stream
      .evalMap { entry =>
        entry.toSeq.map{
          case (month, listings) =>
            val at = new AsciiTable()
            at.addRule()
            at.addRow("Listing ID", "Make", "Selling Price", "Mileage", "Total Amount of contacts")

            listings.foreach{
              case (listing, count) =>
                at.addRule()
                at.addRow(listing.id, listing.make, "€" + listing.price + ",-", listing.mileage, count)

            }
            at.addRule()
            logger.info(s"\nMonth: $month\n${at.render()}")
        }.sequence.void
      }
}
