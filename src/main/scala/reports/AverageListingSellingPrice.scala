package reports

import cats.effect.IO
import de.vandermeer.asciitable.AsciiTable
import fs2.Stream
import io.chrisdavenport.log4cats.Logger

object AverageListingSellingPrice {

  import datatypes._

  def report(listingStream: Stream[IO, Option[Listing]])(implicit logger: Logger[IO]) =
    generate(listingStream).through(output _)

  def generate(listingStream: Stream[IO, Option[Listing]])(implicit logger: Logger[IO]) =
    listingStream.fold(Map.empty[String, (Int, Int)]) {
      (acc, listing) => {
        listing.map { listing => acc.updatedWith(listing.sellerType) {
          previous => Some(previous.fold(listing.price, 1) {
            case (price, count) => (price + listing.price, count + 1)})
        }
        }.getOrElse(acc)
      }
    }.map { entry  =>
       entry.map {
         case (sellerType,(sumPrices, count)) => sellerType  -> (sumPrices / count)
       }
    }

  def output(stream: Stream[IO, Map[String, Int]])(implicit logger: Logger[IO]): Stream[IO, Unit] = {
    val at = new AsciiTable()

    at.addRule()
    at.addRow("Seller Type", "Average in Euro")

    stream.evalMap { entry =>
      entry.foreach {
        case (sellerType, amount) =>
          at.addRule()
          at.addRow(sellerType, "€" +amount.toString + ",-")
      }
      at.addRule()

      logger.info(s"\n${at.render()}")
    }
  }
}
