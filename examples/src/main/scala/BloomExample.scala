
import cats.effect._
import io.chrisdavenport.probabilistic.BloomFilter

object BloomExample extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {
    val x = "Foo"
    for {
      bf <- BloomFilter.string[IO](numberOfItems = 10000,  falsePositiveRate = 0.01)
      present1 <- bf.mayContain(x) // False - It hasn't been inserted yet
      _ <- IO(println(present1))
      _ <- bf.add(x)
      present2 <- bf.mayContain(x) // True - It was inserted
      _ <- IO(println(present2))
    } yield ExitCode.Success
  }

}