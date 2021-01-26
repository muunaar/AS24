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

  def formatDate(date: Long): String = {
    val d : Date = new Date(date)
    new SimpleDateFormat("MM.yyyy").format(d)
  }

  def report(stream: Stream[IO, Option[Contact]])(
    implicit logger: Logger[IO], cache: SignallingRef[IO, Map[Int, Listing]]
  ) = generate(stream).through(stream => join(stream)).through(stream => output(stream))

  def generate(stream: Stream[IO, Option[Contact]])(
    implicit logger: Logger[IO]) = {

    //(date, (idlisting, count))
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

  //(Month, List[(Listings, contactRatecount)])
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
