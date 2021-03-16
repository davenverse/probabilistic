import io.chrisdavenport.probabilistic.CuckooFilter
import cats.effect._

object CuckooExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val x = "Foo"
    for {
      cf <- CuckooFilter.string[IO](numberOfItems = 10000,  falsePositiveRate = 0.01)
      present1 <- cf.mayContain(x) // False - It hasn't been inserted yet
      _ <- IO(println(present1))
      _ <- cf.add(x)
      present2 <- cf.mayContain(x) // True - It was inserted
      _ <- IO(println(present2))
      _ <- cf.remove(x)
      present3 <- cf.mayContain(x) // False - It was removed again. Cool!
      _ <- IO(println(present3))
    } yield ExitCode.Success
  }

}