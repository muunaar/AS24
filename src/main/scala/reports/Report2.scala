package reports

import cats.data.NonEmptyList
import cats.effect.IO
import com.github.gekomad.ittocsv.core.ParseFailure
import datatypes.Listing

object Report2 {

  def percentageDistributionPerMake (stream : fs2.Stream[IO, Either[NonEmptyList[ParseFailure], Listing]]) : fs2.Stream[IO, Unit] =
    stream.fold((0,Map.empty[String, Int])) {
      (acc, listing) =>
        listing match {
          case Right(value) => {
            (acc._1 + 1, acc._2.updatedWith(value.make) {
              previous => Some(previous.fold(0)(prev => prev + 1))
            })

          }
          case Left(_) => acc
        }
    }
      .map(entries => entries._2.foldLeft(Map.empty[String, Float]) {
        (a, el) =>
          println((el._2 + entries._1 ))
          a ++ Map(el._1 -> (el._2 * 100 )/ entries._1.toFloat)})
      .evalMap(dist => IO(println(dist)) )
}
