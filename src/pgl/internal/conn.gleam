import gleam/int
import gleam/option.{type Option, None, Some}
import pgl/internal/socket.{type Socket}

const starting_index = 1

const savepoint_name = "pgl_savepoint"

pub type Conn {
  Conn(sock: Socket, savepoint: Option(Int))
}

pub fn new(sock: Socket) -> Conn {
  Conn(sock:, savepoint: None)
}

pub fn next_savepoint(conn: Conn) -> Conn {
  let savepoint = case conn.savepoint {
    Some(num) -> Some(num + 1)
    None -> Some(starting_index)
  }

  Conn(..conn, savepoint:)
}

pub fn prev_savepoint(conn: Conn) -> Conn {
  let savepoint = case conn.savepoint {
    Some(num) ->
      case num > starting_index {
        True -> Some(num - 1)
        False -> None
      }
    None -> None
  }

  Conn(..conn, savepoint:)
}

pub fn savepoint_statement(conn: Conn) -> String {
  let num = case conn.savepoint {
    Some(num) -> num
    None -> starting_index
  }

  "SAVEPOINT " <> savepoint_name <> int.to_string(num)
}

pub fn release_savepoint_statement(conn: Conn) -> Result(String, Nil) {
  case conn.savepoint {
    Some(num) -> {
      let stmt = "RELEASE SAVEPOINT " <> savepoint_name <> int.to_string(num)

      Ok(stmt)
    }
    None -> Error(Nil)
  }
}

pub fn rollback_savepoint_statement(conn: Conn) -> Result(String, Nil) {
  case conn.savepoint {
    Some(num) -> {
      let stmt =
        "ROLLBACK TO SAVEPOINT " <> savepoint_name <> int.to_string(num) <> ";"

      Ok(stmt)
    }
    None -> Error(Nil)
  }
}
