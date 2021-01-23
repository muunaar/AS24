package reports

import cats.data.NonEmptyList
import cats.effect.IO
import com.github.gekomad.ittocsv.core.ParseFailure
import datatypes.Listing

object Report1 {

  /** calculating the average selling price per seller type
   * @param stream Stream of Listing
   * @return Map identified by Seller type and the associated average selling price
   */
  def averageSellingPrice(stream : fs2.Stream[IO, Either[NonEmptyList[ParseFailure], Listing]]): fs2.Stream[IO, Unit] = stream.fold(Map.empty[String, (Int, Int)]) {
    (acc, listing) =>
      listing match {
        case Right(value) => acc.updatedWith(value.sellerType) {
          previous => Some(previous.fold((value.price, 0))(prev => (prev._1 + value.price, prev._2 + 1)))
        }
        case Left(_) => acc
      }
  }
    .map(entry => entry.map{ case (k,v) => Map(k -> v._1 / v._2)})
    .evalMap(entries => IO(println(entries)))
}
