import args
import config
import gleam/string
import logging

pub fn main() -> Nil {
  logging.configure()

  let assert Ok(args) = args.run()
  let assert Ok(config) = config.load_config(args.config_file)

  logging.log(logging.Info, string.inspect(config))
}
