import args
import call
import config
import database
import event
import gleam/bit_array
import gleam/bytes_tree
import gleam/erlang/atom
import gleam/erlang/node
import gleam/erlang/process
import gleam/http
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/httpc
import gleam/json
import gleam/result
import gleam/string
import gmnesia/read
import gmnesia/schema
import gmnesia/system
import gmnesia/table
import gmnesia/table/create as table_create
import gmnesia/table/wait
import gmnesia/transaction
import gmnesia/write
import logging
import mist.{type Connection, type ResponseData}

pub fn main() -> Nil {
  logging.configure()

  let assert Ok(args) = args.run()
  let assert Ok(config) = config.load_config(args.config_file)

  logging.log(logging.Info, string.inspect(config))

  let assert Ok(_) = system.stop()

  let _ = schema.create_schema(nodes: [node.name(node.self())])

  let assert Ok(_) = system.start()

  let table = atom.create("event")

  logging.log(logging.Info, "Creating table")

  let _ =
    table
    |> table_create.new
    |> table_create.options([
      table.Attributes([atom.create("id"), atom.create("data")]),
      table.Type(table.Set),
    ])
    |> table_create.create

  logging.log(logging.Info, "Waiting for table to be ready")

  let assert Ok(_) =
    [table]
    |> wait.new
    |> wait.timeout(table.Finite(5000))
    |> wait.wait

  logging.log(logging.Info, "Starting server")

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

    let database_event = database.from_event(event)

    logging.log(logging.Debug, string.inspect(database_event))

    let table = atom.create("event")

    use database_events <- result.try(
      fn() { table |> read.new(key: database_event.id) |> read.read }
      |> transaction.new
      |> transaction.execute
      |> result.map_error(fn(_) { Nil }),
    )

    case database_events {
      [event, ..] -> {
        logging.log(
          logging.Info,
          "Duplicate event: "
            <> string.inspect(event)
            <> " for call: "
            <> string.inspect(call),
        )

        Ok(response.new(200) |> response.set_body(mist.Bytes(bytes_tree.new())))
      }
      [] -> {
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

        logging.log(
          logging.Info,
          "Writing event: " <> string.inspect(database_event),
        )

        use _ <- result.try(
          fn() {
            database_event
            |> write.new
            |> write.write
          }
          |> transaction.new
          |> transaction.execute
          |> result.map_error(fn(err) {
            logging.log(
              logging.Error,
              "Failed to write event: "
                <> string.inspect(database_event)
                <> " for call: "
                <> string.inspect(call)
                <> " err: "
                <> string.inspect(err),
            )
            Nil
          }),
        )

        Ok(response.new(200) |> response.set_body(mist.Bytes(bytes_tree.new())))
      }
    }
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
