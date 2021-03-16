
import cats.effect._
import io.chrisdavenport.probabilistic.BloomFilter

object BloomExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val x = "Foo"
    for {
      bf <- BloomFilter.string[IO](10000, 0.01)
      present1 <- bf.mayContain(x)
      _ <- IO(println(present1))
      _ <- bf.add(x)
      present2 <- bf.mayContain(x)
      _ <- IO(println(present2))
    } yield ExitCode.Success
  }

}