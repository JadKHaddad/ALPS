import args
import call
import config
import database
import event
import gleam/bit_array
import gleam/bytes_tree
import gleam/dynamic/decode
import gleam/erlang/atom
import gleam/erlang/node
import gleam/erlang/process
import gleam/http
import gleam/http/request.{type Request}
import gleam/http/response.{type Response}
import gleam/httpc
import gleam/json
import gleam/list
import gleam/result
import gleam/string
import gmnesia/config as gmnesia_config
import gmnesia/debug
import gmnesia/lock
import gmnesia/read
import gmnesia/schema
import gmnesia/storage
import gmnesia/system
import gmnesia/table
import gmnesia/table/create as table_create
import gmnesia/table/wait
import gmnesia/transaction
import gmnesia/unsplit
import gmnesia/unsplit/server as unsplit_server
import gmnesia/write
import logging
import mist.{type Connection, type ResponseData}

pub fn main() -> Nil {
  logging.configure()
  logging.set_level(logging.Debug)

  debug.set_debug_level(debug.Verbose)

  let assert Ok(args) = args.run()
  let assert Ok(config) = config.load_config(args.config_file)

  logging.log(logging.Info, "Args: " <> string.inspect(args))
  logging.log(logging.Info, "Config: " <> string.inspect(config))

  let self_node = node.name(node.self())
  let table_name = atom.create("event")
  let schema_name = atom.create("schema")

  case args.bootstrap {
    True -> {
      logging.log(logging.Info, "Bootstrap mode: Initializing...")

      let _ = system.stop()

      case schema.create_schema([self_node]) {
        Ok(_) -> logging.log(logging.Info, "Schema created")
        Error(_) -> logging.log(logging.Warning, "Schema already exists")
      }

      let assert Ok(_) = system.start()

      let _ =
        table_name
        |> table_create.new
        |> table_create.options([
          table.Attributes([atom.create("id"), atom.create("data")]),
          table.Type(table.Set),
          table.DiscCopies([self_node]),
          table.UserProperties([unsplit.unsplit_bag()]),
        ])
        |> table_create.create

      logging.log(
        logging.Info,
        "Bootstrap complete. Restart without bootstrap: True.",
      )
    }

    False -> {
      logging.log(logging.Info, "Cluster join mode")

      let reachable_nodes = connect_to_other_nodes(config)

      let assert Ok(_) = system.start()

      logging.log(
        logging.Info,
        "Attempting to join cluster nodes: " <> string.inspect(reachable_nodes),
      )

      case gmnesia_config.change_config_extra_db_nodes(reachable_nodes) {
        Ok(nodes) ->
          logging.log(
            logging.Info,
            "Joined cluster nodes: " <> string.inspect(nodes),
          )
        Error(e) ->
          logging.log(logging.Error, "Failed to join: " <> string.inspect(e))
      }

      case
        table.change_table_copy_type(schema_name, self_node, storage.DiscCopies)
      {
        Ok(_) ->
          logging.log(logging.Info, "Local schema promoted to DiscCopies")
        Error(e) ->
          logging.log(
            logging.Warning,
            "Schema promotion skipped: " <> string.inspect(e),
          )
      }

      let _ = [schema_name] |> wait.new |> wait.wait

      wait_for_discovery(table_name)

      case table.add_table_copy(table_name, self_node, storage.DiscCopies) {
        Ok(_) -> logging.log(logging.Info, "Table copy added!")
        Error(e) ->
          logging.log(logging.Error, "Copy error: " <> string.inspect(e))
      }

      let _ = [table_name] |> wait.new |> wait.wait

      logging.log(logging.Info, "Cluster join complete")

      let assert Ok(_) = unsplit_server.start()

      run_server(config)
    }
  }
}

fn other_nodes(config: config.Config) -> List(atom.Atom) {
  let self_node = node.name(node.self())

  config.nodes
  |> list.map(atom.create)
  |> list.filter(fn(n) { n != self_node })
}

fn connect_to_other_nodes(config: config.Config) -> List(atom.Atom) {
  let other_nodes = other_nodes(config)

  logging.log(
    logging.Info,
    "Connecting to other nodes: " <> string.inspect(other_nodes),
  )

  other_nodes
  |> list.filter(fn(n) {
    case node.connect(n) {
      Ok(_) -> True
      Error(_) -> False
    }
  })
}

fn connect_to_other_nodes_or_fail(config: config.Config) -> Result(Nil, String) {
  let expected_nodes = other_nodes(config)
  let _ = connect_to_other_nodes(config)

  let unreachable =
    expected_nodes
    |> list.filter(fn(n) { !is_node_alive(n) })

  case unreachable {
    [] -> Ok(Nil)
    _ ->
      Error(
        "Could not connect to all cluster nodes. Missing: "
        <> string.inspect(unreachable),
      )
  }
}

fn wait_for_discovery(table_name: atom.Atom) {
  let known_tables = system.system_info(system.Tables)

  logging.log(logging.Info, "Known tables: " <> string.inspect(known_tables))

  let assert Ok(known_tables) =
    decode.run(known_tables, decode.list(decode.dynamic))

  let known_tables = list.map(known_tables, atom.cast_from_dynamic)

  case list.contains(known_tables, table_name) {
    True -> logging.log(logging.Info, "Table discovered in schema")
    False -> {
      logging.log(logging.Info, "Table not in schema yet. Retrying...")

      process.sleep(1000)

      wait_for_discovery(table_name)
    }
  }
}

fn run_server(config: config.Config) -> Nil {
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

type WorkError {
  BadRequest
  Unavailable
}

fn empty_response(status: Int) -> Response(ResponseData) {
  response.new(status)
  |> response.set_body(mist.Bytes(bytes_tree.new()))
}

fn handle_work(
  req: Request(Connection),
  config: config.Config,
) -> Response(ResponseData) {
  case process_work(req, config) {
    Ok(Nil) -> empty_response(200)
    Error(BadRequest) -> empty_response(400)
    Error(Unavailable) -> empty_response(500)
  }
}

fn process_work(
  req: Request(Connection),
  config: config.Config,
) -> Result(Nil, WorkError) {
  use _ <- result.try(
    connect_to_other_nodes_or_fail(config)
    |> result.map_error(fn(_) { Unavailable }),
  )

  use req <- result.try(
    mist.read_body(req, 1024 * 1024 * 10)
    |> result.map_error(fn(_) { BadRequest }),
  )

  use body_string <- result.try(
    req.body
    |> bit_array.to_string
    |> result.map_error(fn(_) { BadRequest }),
  )

  use simple_call <- result.try(
    json.parse(body_string, call.decoder())
    |> result.map_error(fn(_) { BadRequest }),
  )

  logging.log(logging.Debug, string.inspect(simple_call))

  use parsed_event <- result.try(
    event.from_call(simple_call)
    |> result.map_error(fn(_) { BadRequest }),
  )

  let database_event = database.from_event(parsed_event)

  logging.log(
    logging.Debug,
    "Checking for duplicate event: " <> string.inspect(database_event),
  )

  use exists <- result.try(
    event_exists(database_event.id)
    |> result.map_error(fn(_) { Unavailable }),
  )

  case exists {
    True -> {
      logging.log(
        logging.Warning,
        "Duplicate event: "
          <> string.inspect(database_event)
          <> " for call: "
          <> string.inspect(simple_call),
      )
      Ok(Nil)
    }

    False -> {
      logging.log(
        logging.Info,
        "No duplicate event found. Posting call: "
          <> string.inspect(simple_call),
      )
      use _ <- result.try(
        post_call(simple_call, config)
        |> result.map_error(fn(err) {
          logging.log(
            logging.Error,
            "Failed to post call: "
              <> string.inspect(simple_call)
              <> " err: "
              <> err,
          )
          Unavailable
        }),
      )

      logging.log(
        logging.Info,
        "Writing event: " <> string.inspect(database_event),
      )

      case record_event_with_retry(database_event, 10) {
        Ok(_) -> Nil
        Error(_) ->
          // The server already has the call. Answering 500 would guarantee
          // a redelivery and therefore a duplicate, so we answer 200.
          logging.log(
            logging.Error,
            "Call was posted but event could not be recorded: "
              <> string.inspect(database_event),
          )
      }

      Ok(Nil)
    }
  }
}

fn event_exists(id) -> Result(Bool, Nil) {
  let table = atom.create("event")

  fn() {
    let events =
      table
      |> read.new(key: id)
      |> read.lock(lock.Write)
      |> read.read

    !list.is_empty(events)
  }
  |> transaction.new
  |> transaction.execute
  |> result.map_error(fn(_) { Nil })
}

fn record_event(database_event) -> Result(Nil, Nil) {
  fn() {
    let _ =
      database_event
      |> write.new
      |> write.write

    Nil
  }
  |> transaction.new
  |> transaction.execute
  |> result.map_error(fn(_) { Nil })
}

fn record_event_with_retry(
  database_event,
  attempts_left: Int,
) -> Result(Nil, Nil) {
  case record_event(database_event) {
    Ok(_) -> Ok(Nil)
    Error(_) if attempts_left > 1 -> {
      logging.log(
        logging.Warning,
        "Recording event failed, retrying. Attempts left: "
          <> string.inspect(attempts_left - 1),
      )
      process.sleep(200)
      record_event_with_retry(database_event, attempts_left - 1)
    }
    Error(_) -> Error(Nil)
  }
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

@external(erlang, "net_adm", "ping")
fn net_adm_ping(node: atom.Atom) -> atom.Atom

fn is_node_alive(n: atom.Atom) -> Bool {
  net_adm_ping(n) == atom.create("pong")
}
