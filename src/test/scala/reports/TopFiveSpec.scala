package reports

import cats.effect.IO
import datatypes.{Contact, Listing}
import fs2.Stream
import fs2.concurrent.SignallingRef
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger
import org.scalatest.flatspec.AnyFlatSpec

class TopFiveSpec extends AnyFlatSpec{

  import TopFive._

  implicit val logger = Slf4jLogger.getLoggerFromName[IO]("Test")
  implicit val cs = IO.contextShift(scala.concurrent.ExecutionContext.Implicits.global)

  "Format date from Long" should "return a String as MM.YYYY" in {
    assert(TopFive.formatDate(1585379517000L) == "03.2020")
  }

  "Grouping by month" should "a Map collection having as Key the month and Value a Map of ListingId and their count" in {

    val contacts =  Stream(
      Some(Contact(1286, 1592498493000L)),
      Some(Contact(1288, 1588508838000L)),
      Some(Contact(1284, 1592850063000L)),
      Some(Contact(1285, 1580893831000L)),
      Some(Contact(1280, 1592498493000L)),
      Some(Contact(1280, 1592498493000L)),
      Some(Contact(1280, 1592498493000L)),
      Some(Contact(1288, 1581412353000L)),
      Some(Contact(1289, 1581732589000L)),
      Some(Contact(1291, 1581586621000L)),
      Some(Contact(1292, 1585904051000L)),
      Some(Contact(1292, 1585904051000L)),
      Some(Contact(1288, 1580893831000L)),
      Some(Contact(1289, 1588030368000L)),
      Some(Contact(1289, 1588030368000L)),
      Some(Contact(1289, 1588030368000L)))

    assert(generate(contacts).compile.toList.unsafeRunSync() ==
      List(
        Map(
          "06.2020" -> Map(1286 -> 1, 1284  -> 1 , 1280 -> 3),
          "05.2020" -> Map(1288 -> 1),
          "02.2020" -> Map(1285 -> 1, 1288 -> 2, 1289 -> 1, 1291 -> 1),
          "04.2020" -> Map(1292 -> 2, 1289 -> 3)
      )))
  }

  "Extracting related listings info " should  "return a Map" in {

    val contacts = Stream(Map(
      "06.2020" -> Map(1286 -> 1, 1284  -> 1 , 1280 -> 3),
      "05.2020" -> Map(1288 -> 1),
      "02.2020" -> Map(1285 -> 1, 1288 -> 2, 1289 -> 1, 1291 -> 1),
      "04.2020" -> Map(1292 -> 2, 1289 -> 3)))

    implicit val listing = SignallingRef[IO, Map[Int, Listing]](Map(
      1286 -> Listing(1286,"BWM",23002,2000,"dealer"),
      1288  -> Listing(1288,"VW",48205,4500,"other"),
      1292 -> Listing(1292,"Mazda",42531,6500,"private"),
      1284 -> Listing(1284,"Renault",38361,1000,"other"),
      1289 -> Listing(1289,"BWM",23002,2000,"dealer"),
      1285 -> Listing(1285,"VW",48205,4500,"other"),
      1291 -> Listing(1291,"Mazda",42531,6500,"private"),
      1280 -> Listing(1280,"Renault",38361,1000,"other"))).unsafeRunSync()

    assert(
      join(contacts).compile.toList.map(_.map(_.toMap)).unsafeRunSync() == List(Map(
        ("06.2020" , List(
        (Listing(1280,"Renault",38361,1000,"other"),3),
        (Listing(1286,"BWM",23002,2000,"dealer"),1),
        (Listing(1284,"Renault",38361,1000,"other"),1))),
        ("05.2020", List(
          (Listing(1288,"VW",48205,4500,"other"),1))),
        ("02.2020" , List(
          (Listing(1288,"VW",48205,4500,"other"),2),
          (Listing(1285,"VW",48205,4500,"other"),1),
          (Listing(1289,"BWM",23002,2000,"dealer"),1),
          (Listing(1291,"Mazda",42531,6500,"private"),1))),
        ("04.2020" , List(
        (Listing(1289,"BWM",23002,2000,"dealer"),3),
        (Listing(1292,"Mazda",42531,6500,"private"),2))))))
  }
}
