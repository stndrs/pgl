import pgl/internal/conn
import pgl/internal/socket_test

pub fn savepoint_statement_test() {
  let conn = connection()

  assert "SAVEPOINT pgl_savepoint1" == conn.savepoint_statement(conn)
}

pub fn release_savepoint_statement_test() {
  let conn =
    connection()
    |> conn.next_savepoint()

  let assert Ok("RELEASE SAVEPOINT pgl_savepoint1") =
    conn.release_savepoint_statement(conn)
}

pub fn release_savepoint_statement_error_test() {
  let conn = connection()

  let assert Error(Nil) = conn.release_savepoint_statement(conn)
}

pub fn rollback_savepoint_statement_test() {
  let conn =
    connection()
    |> conn.next_savepoint()

  let assert Ok("ROLLBACK TO SAVEPOINT pgl_savepoint1;") =
    conn.rollback_savepoint_statement(conn)
}

pub fn rollback_savepoint_statement_error_test() {
  let conn = connection()

  let assert Error(Nil) = conn.rollback_savepoint_statement(conn)
}

pub fn has_savepoint_false_test() {
  let conn = connection()

  assert !conn.has_savepoint(conn)
}

pub fn has_savepoint_true_test() {
  let conn =
    connection()
    |> conn.next_savepoint

  assert conn.has_savepoint(conn)
}

fn connection() {
  let sock = socket_test.connect()

  conn.new(sock)
}
