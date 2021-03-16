import io.chrisdavenport.probabilistic.CuckooFilter
import cats.effect._

object CuckooExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val x = "Foo"
    for {
      cf <- CuckooFilter.string[IO](10000, 0.01)
      present1 <- cf.mayContain(x)
      _ <- IO(println(present1))
      _ <- cf.add(x)
      present2 <- cf.mayContain(x)
      _ <- IO(println(present2))
      _ <- cf.remove(x)
      present3 <- cf.mayContain(x)
      _ <- IO(println(present3))
    } yield ExitCode.Success
  }

}