package reports

import cats.effect.IO
import de.vandermeer.asciitable.AsciiTable
import fs2.Stream
import io.chrisdavenport.log4cats.Logger

object AverageListingSellingPrice {

  import datatypes._

  def calculate(listingStream: Stream[IO,  Map[Int, Listing]])(implicit logger: Logger[IO]) =
    listingStream.map { listings =>
       listings.view.values.foldLeft(Map.empty[String, (Int, Int)]) { (acc, listing) =>
         acc.updatedWith(listing.sellerType) { previous =>
           Some(previous.fold((listing.price, 1)) { case (price, count) => (price + listing.price, count + 1) })
         }
       }.map { case (sellerType, (price, count)) =>
         sellerType -> price/count
       }
    }.through(report _)

  /** Display the results
   * @param stream : Stream of Map identifies with the sellerType and the average price
   * @param logger : a logger
   * @return displays the Average selling price per SellerType Report
   */
  def report(stream: Stream[IO, Map[String, Int]])(implicit logger: Logger[IO]): Stream[IO, Unit] = {
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
