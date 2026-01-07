import gleam/erlang/process
import gleam/int
import gleam/option.{type Option, None, Some}
import pgl/internal/socket.{type Socket}

pub type Conn {
  Conn(sock: Socket, savepoint: Option(Int), caller: process.Pid)
}

pub fn new(sock: Socket, caller: process.Pid) -> Conn {
  Conn(sock:, savepoint: None, caller:)
}

pub fn next_savepoint(conn: Conn) -> Conn {
  let savepoint = case conn.savepoint {
    Some(num) -> Some(num + 1)
    None -> Some(0)
  }

  Conn(..conn, savepoint:)
}

pub fn prev_savepoint(conn: Conn) -> Conn {
  let savepoint = case conn.savepoint {
    Some(num) -> Some(num - 1)
    None -> None
  }

  Conn(..conn, savepoint:)
}

const savepoint_name = "pgl_savepoint"

pub fn savepoint_statement(conn: Conn) -> String {
  let num = case conn.savepoint {
    Some(num) -> num
    None -> 0
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
