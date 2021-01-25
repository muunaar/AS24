package reports

import cats.effect.IO
import datatypes.Listing
import de.vandermeer.asciitable.AsciiTable
import fs2.Stream
import io.chrisdavenport.log4cats.Logger

object PercentualDistributionPerMake {

  /** calculate the percentual distribution per Make
   * @param listingStream : Stream of Map identified with the listingId.
   * @param logger : logger
   * @return A stream of Map identified with the make and the associated percentual distribution.
   */
  def calculate(listingStream: Stream[IO, Map[Int, Listing]])(implicit logger: Logger[IO]) =
    listingStream
      .fold(0, Map.empty[String, Int]) { (acc, listings) =>
        listings.view.values.foldLeft(acc) { (acc, listing) =>
          (acc._1 + 1, acc._2.updatedWith(listing.make) { previous =>
            Some(previous.fold(1) { count => count + 1 })
          })
        }
      }
      .map { case (total, occurencesPerMake) =>
        occurencesPerMake.map { case (make, count) =>
          make -> (count * 100) / total.toFloat
        }
      }
      .through(report _)

  /** Displays the results
   * @param stream : Stream of Map identifies with the make and the percentual distribution
   * @param logger : a logger
   * @return displays the Percentual distribution per Make report.
   */

    def report(streamListings: Stream[IO, Map[String, Float]])(implicit logger: Logger[IO]): Stream[IO, Unit] = {
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
