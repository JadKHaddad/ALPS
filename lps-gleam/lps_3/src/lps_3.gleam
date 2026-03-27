import args
import call
import config
import event
import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/process
import gleam/http
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/httpc
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
        ["work"] -> handle_work(req, config)
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

fn handle_work(
  req: Request(Connection),
  config: config.Config,
) -> Response(ResponseData) {
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

    logging.log(logging.Debug, string.inspect(call))

    use event <- result.try(
      event.from_call(call)
      |> result.map_error(fn(_) { Nil }),
    )

    logging.log(logging.Debug, string.inspect(event))

    logging.log(logging.Info, "Posting call: " <> string.inspect(call))

    use _ <- result.try(
      post_call(call, config)
      |> result.map_error(fn(err) {
        logging.log(
          logging.Error,
          "Failed to post call: " <> string.inspect(call) <> " err: " <> err,
        )
        Nil
      }),
    )

    Ok(response.new(200) |> response.set_body(mist.Bytes(bytes_tree.new())))
  }

  result.unwrap(
    result,
    response.new(400) |> response.set_body(mist.Bytes(bytes_tree.new())),
  )
}

fn post_call(
  call: call.SimpleCall,
  config: config.Config,
) -> Result(Nil, String) {
  let text = json.to_string(call.to_json(call))

  use req <- result.try(
    result.map_error(request.to(config.server_push_url), fn(_) {
      "Invalid server_push_url"
    }),
  )
  let req = request.prepend_header(req, "content-type", "application/json")
  let req = request.set_method(req, http.Post)
  let req = request.set_body(req, text)
  use resp <- result.try(
    result.map_error(httpc.send(req), fn(err) {
      "Failed to send request: err: " <> string.inspect(err)
    }),
  )

  case resp.status {
    200 -> Ok(Nil)
    code -> Error("Server responded with status code " <> string.inspect(code))
  }
}
