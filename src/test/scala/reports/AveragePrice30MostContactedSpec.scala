package reports

import cats.effect.IO
import datatypes.{Contact, Listing}
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.flatspec.AnyFlatSpec

class AveragePrice30MostContactedSpec  extends AnyFlatSpec{

  import AveragePrice30MostContacted._

  implicit val logger = Slf4jLogger.getLoggerFromName[IO]("Test")
  implicit val cs = IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

  "Grouping listings" should "return a collection of ListingId and their related count" in {

    val contacts =  Stream(
      Some(Contact(1286, 1592498493)),
      Some(Contact(1288, 1588508838)),
      Some(Contact(1284, 1592850063)),
      Some(Contact(1285, 1580893831)),
      Some(Contact(1280, 1592498493)),
      Some(Contact(1288, 1581412353)),
      Some(Contact(1289, 1581732589)),
      Some(Contact(1291, 1581586621)),
      Some(Contact(1292, 1585904051)),
      Some(Contact(1288, 1580893831)),
      Some(Contact(1289, 1588030368)))

    assert(AveragePrice30MostContacted.generate(contacts).compile.toList.unsafeRunSync() == List(
      Seq((1288, 3),(1289, 2))))
  }

  "Average Price of the Thirty percent of most contacted listings" should "the average Price of 30% contacts" in {

    implicit val listing = SignallingRef[IO, Map[Int, Listing]](Map(
      1286 -> Listing(1286,"BWM",23002,2000,"dealer"),
      1288  -> Listing(1288,"VW",48205,4500,"other"),
      1292 -> Listing(1292,"Mazda",42531,6500,"private"),
      1284 -> Listing(1284,"Renault",38361,1000,"other"),
      1289 -> Listing(1289,"BWM",23002,2000,"dealer"),
      1285 -> Listing(1285,"VW",48205,4500,"other"),
      1291 -> Listing(1291,"Mazda",42531,6500,"private"),
      1280 -> Listing(1280,"Renault",38361,1000,"other"))).unsafeRunSync()

    val mostContacted =  Stream(Seq((1288, 3),(1289, 2)))

    assert(join(mostContacted).compile.toList.unsafeRunSync() == List(35603))
  }
}
