package reports

import cats.effect.IO
import org.scalatest.flatspec.AnyFlatSpec
import fs2.Stream
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

class AverageListingSellingPriceSpec extends AnyFlatSpec {

  import datatypes.Listing

  implicit val logger = Slf4jLogger.getLoggerFromName[IO]("Test")

  "Average Listing price per Seller type" should "A Map of sellerType with the average listing price " in {
    val listings : Stream[IO, Option[Listing]] = Stream(
      Some(Listing(1286,"BWM",23002,2000,"dealer")),
      Some(Listing(1288,"VW",48205,4500,"other")),
      Some(Listing(1292,"Mazda",42531,6500,"private")),
      Some(Listing(1284,"Renault",38361,1000,"other")))

    assert(AverageListingSellingPrice.generate(listings).compile.toList.unsafeRunSync() == List(
      Map("dealer" -> 23002, "other" -> 43283, "private" -> 42531)))
  }
}
