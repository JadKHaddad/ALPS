import args
import call
import config
import event
import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/process
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/json
import gleam/result
import gleam/string
import logging
import mist.{type Connection, type ResponseData}

pub fn main() -> Nil {
  logging.configure()

  let assert Ok(args) = args.run()
  let assert Ok(config) = config.load_config(args.config_file)

  logging.log(logging.Info, string.inspect(config))

  let assert Ok(_) =
    fn(req: Request(Connection)) -> Response(ResponseData) {
      case request.path_segments(req) {
        ["work"] -> handle_work(req)
        _ ->
          response.new(404)
          |> response.set_body(mist.Bytes(bytes_tree.new()))
      }
    }
    |> mist.new
    |> mist.bind("0.0.0.0")
    |> mist.port(config.port)
    |> mist.start

  process.sleep_forever()
}

fn handle_work(req: Request(Connection)) -> Response(ResponseData) {
  let result = {
    use req <- result.try(
      mist.read_body(req, 1024 * 1024 * 10)
      |> result.map_error(fn(_) { Nil }),
    )

    use body_string <- result.try(
      req.body
      |> bit_array.to_string
      |> result.map_error(fn(_) { Nil }),
    )

    use call <- result.try(
      json.parse(body_string, call.decoder())
      |> result.map_error(fn(_) { Nil }),
    )

    logging.log(logging.Info, string.inspect(call))

    use event <- result.try(
      event.from_call(call)
      |> result.map_error(fn(_) { Nil }),
    )

    logging.log(logging.Info, string.inspect(event))

    Ok(response.new(200) |> response.set_body(mist.Bytes(bytes_tree.new())))
  }

  result.unwrap(
    result,
    response.new(400) |> response.set_body(mist.Bytes(bytes_tree.new())),
  )
}
