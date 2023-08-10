package streaming

import cats.effect.IO
import cats.effect.IOApp
import fs2.Stream
import io.circe.syntax.EncoderOps
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.dsl.io._
import org.http4s.HttpRoutes

import scala.concurrent.duration.DurationInt
import scala.util.Random

object HttpServeStream extends IOApp.Simple {

  /** 5 elements via 1 second */
  val stream: Stream[IO, Data] =
    Stream
      .awakeEvery[IO](1.second)
      .map(_ => Data(Random.nextInt(100)))
      .take(5)

  // pack them as array
  import org.http4s.circe.streamJsonArrayEncoder
  val route: HttpRoutes[IO] = HttpRoutes.of[IO] {
    /** just test response */
    case GET -> Root / "t" => Ok("test")
    /** serve them as separate elements */
    case GET -> Root / "s" => Ok(stream.map(_.asJson.noSpaces))
    /** serve them as an array */
    case GET -> Root / "a" => Ok(stream.map(_.asJson))
  }

  override def run = BlazeServerBuilder[IO]
    .bindHttp(8080, "localhost")
    .withHttpApp(route.orNotFound)
    .serve
    .compile
    .drain
}
