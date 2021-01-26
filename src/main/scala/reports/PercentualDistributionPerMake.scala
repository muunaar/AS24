package reports

import cats.effect.IO
import datatypes.Listing
import de.vandermeer.asciitable.AsciiTable
import fs2.Stream
import io.chrisdavenport.log4cats.Logger

object PercentualDistributionPerMake {

  def report(listingStream: Stream[IO, Option[ Listing]])(implicit logger: Logger[IO]) =
    generate(listingStream).through(stream  => output (stream))

  def generate(listingStream: Stream[IO, Option[ Listing]])(implicit logger: Logger[IO]) =
    listingStream
      .fold(0, Map.empty[String, Int]) { (acc, listings) =>
        listings.map{
          listing => (acc._1 + 1 , acc._2.updatedWith(listing.make) {
            prev => Some(prev.fold(1) {existing => existing + 1})
          })
        }.getOrElse(acc)
      }
      .map{ case (total, occurencesPerMake) =>
        occurencesPerMake.map { case (make, count) =>
          make -> (count* 100)/ total.toFloat
        }
      }

  def output(streamListings: Stream[IO, Map[String, Float]])(implicit logger: Logger[IO]) = {
    val at = new AsciiTable()
    at.addRule()
    at.addRow("Make", "Distribution")
    streamListings.evalMap { entry =>
      entry.foreach {
        case (make, dist) =>
          at.addRule()
          at.addRow(make, dist.toString + "%")
      }
      at.addRule()
      logger.info(s"\n${at.render()}")
    }
  }
}
