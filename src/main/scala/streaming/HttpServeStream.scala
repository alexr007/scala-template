package streaming

import cats.effect.IO
import cats.effect.IOApp
import fs2.Stream
import io.circe.Json
import io.circe.syntax.EncoderOps
import org.http4s.HttpRoutes
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.dsl.io._
import scala.concurrent.duration.DurationInt
import scala.util.Random

object HttpServeStream extends IOApp.Simple {

  /** 5 elements via 1 second */
  val datas: Stream[IO, Data] =
    Stream
      .awakeEvery[IO](1.second)
      .map(_ => Data(Random.nextInt(100)))
      .take(5)

  val jsons: Stream[IO, Json] =
    datas.map(_.asJson)

  val strings: Stream[IO, String] =
    jsons.map(_.noSpaces)

  val route: HttpRoutes[IO] = HttpRoutes.of[IO] {
    /** just test response */
    case GET -> Root / "t" =>
      import org.http4s.circe.jsonEncoder
      Ok("test".asJson)

    /** serve them as an array */
    case GET -> Root / "a" =>
      import org.http4s.circe.streamJsonArrayEncoder
      Ok(jsons)

    /** serve them as separate elements */
    case GET -> Root / "s" =>
      Ok(strings)

  }

  override def run = BlazeServerBuilder[IO]
    .bindHttp(8080, "localhost")
    .withHttpApp(route.orNotFound)
    .serve
    .compile
    .drain
}
