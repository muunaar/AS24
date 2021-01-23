package core

import cats.data.NonEmptyList
import cats.effect.{ExitCode, IO, IOApp}
import com.github.gekomad.ittocsv.core.ParseFailure
import com.github.gekomad.ittocsv.parser.IttoCSVFormat
import com.github.gekomad.ittocsv.parser.io.FromFile.csvFromFileStream
import datatypes.{Contact, Listing}
import reports.Report1.averageSellingPrice

object Main extends IOApp {

 implicit val csvFormat: IttoCSVFormat = com.github.gekomad.ittocsv.parser.IttoCSVFormat.default

 val listingStream: fs2.Stream[IO, Either[NonEmptyList[ParseFailure], Listing]] =
  csvFromFileStream[Listing]("data/listings.csv", skipHeader = true).dropLast

 val contactsStream: fs2.Stream[IO, Either[NonEmptyList[ParseFailure], Contact]] =
  csvFromFileStream[Contact]("data/contacts.csv", skipHeader = true).dropLast

 override def run(args: List[String]): IO[ExitCode] =
  averageSellingPrice(listingStream).compile.drain.as(ExitCode.Success)

}
