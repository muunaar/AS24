package reports

import cats.effect.IO
import datatypes.Listing
import fs2.Stream
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.flatspec.AnyFlatSpec

class PercentualDistributionPerMakeSpec extends AnyFlatSpec {

  implicit val logger = Slf4jLogger.getLoggerFromName[IO]("Test")

  "The percentual distribution per Make" should "return" in {

    val listings = Stream(
      Some(Listing(1286,"BWM",23002,2000,"dealer")),
      Some(Listing(1288,"VW",48205,4500,"other")),
      Some(Listing(1292,"Mazda",42531,6500,"private")),
      Some(Listing(1284,"Renault",38361,1000,"other"))
    )
    assert(PercentualDistributionPerMake.generate(listings).compile.toList.unsafeRunSync() == List(
      Map("BWM" -> 25.0, "VW" -> 25.0, "Mazda" -> 25.0, "Renault" -> 25.0)
    ))
  }
}
