package core

import cats.effect._
import cats.implicits._
import com.github.gekomad.ittocsv.parser.IttoCSVFormat
import com.github.gekomad.ittocsv.parser.io.FromFile.csvFromFileStream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

object Main extends IOApp{

 import datatypes._
 import reports._

 def mainStream(implicit logger: Logger[IO], cache: SignallingRef[IO, Map[Int, Listing]]) = {

  implicit val csvFormat: IttoCSVFormat = com.github.gekomad.ittocsv.parser.IttoCSVFormat.default

  def listings =
   csvFromFileStream[Listing]("data/listings.csv", skipHeader = true)
     .dropLast
     .evalMap {
      case Right(listing) =>
       cache.update( _ + (listing.id -> listing)) >> Some(listing).pure[IO]
      case Left(err) => logger.error(err.toString())  >>  None.pure[IO]
     }
     .broadcastTo(
      stream => AverageListingSellingPrice.report(stream),
      stream => PercentualDistributionPerMake.report(stream)
     )

  def contacts =
   csvFromFileStream[Contact]("data/contacts.csv", skipHeader = true)
     .dropLast
     .evalMap {
      case Right(contact) => Some(contact).pure[IO]
      case Left(err) => logger.error(err.toString()).flatMap(_ => None.pure[IO])
     }
     .broadcastTo(stream => AveragePrice30MostContacted.report(stream),
      stream => TopFive.report(stream))

  listings interleaveAll contacts
 }

 override def run(args: List[String]): IO[ExitCode] = {
  for {
   implicit0(cache : SignallingRef[IO, Map[Int, Listing]]) <- SignallingRef[IO, Map[Int, Listing]](Map.empty)
   implicit0(logger : Logger[IO]) <- Slf4jLogger.create[IO]
   _  <- mainStream.compile.drain
  } yield (ExitCode.Success)

 }
}
