package core

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits.{catsSyntaxApplicativeId, catsSyntaxFlatMapOps}
import com.github.gekomad.ittocsv.parser.IttoCSVFormat
import com.github.gekomad.ittocsv.parser.io.FromFile._
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object Main extends IOApp{

 import reports._
 import datatypes._

 implicit val csvFormat: IttoCSVFormat = com.github.gekomad.ittocsv.parser.IttoCSVFormat.default

 /** reading the listings csv file as a Stream
  * @return Stream of Map of listingId and the related Listing information.
  */
 def listings = {
  csvFromFileStream[Listing]("data/listings.csv", skipHeader = true)
    .dropLast
    .fold(Map.empty[Int, Listing]) {
     (acc, listing) =>
      listing match {
       case Right(l) => acc + (l.id -> l)
       case Left(_) => acc
      }
    }
 }

 /** reading the listings csv file as a Stream
  * @return Stream of Map of ListingId and the related Listing information
  */
 def contacts(implicit logger : Logger[IO]) =
  csvFromFileStream[Contact]("data/contacts.csv", skipHeader = true)
    .dropLast
    .evalMap {
     case Right(contact) => Some(contact).pure[IO]
     case Left(err) => logger.error(err.toString()).flatMap(_ => None.pure[IO])
    }

 def mainStream(implicit logger: Logger[IO]) = {

  val listingsStream = listings
  listingsStream.broadcastTo(AverageListingSellingPrice.calculate _ )
 }

 override def run(args: List[String]): IO[ExitCode] = {
  Slf4jLogger.create[IO] >>= { implicit logger =>
   mainStream.compile.drain.as(ExitCode.Success)
  }
 }
}