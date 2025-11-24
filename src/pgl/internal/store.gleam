import exception
import gleam/dict
import gleam/erlang/atom.{type Atom}
import gleam/int
import gleam/result

pub type Store(a, b) {
  Store(
    set: fn(a, fn(b) -> b) -> Result(b, StoreError),
    insert: fn(a, b) -> #(a, b),
    lookup: fn(a) -> Result(b, StoreError),
    delete: fn() -> Nil,
  )
}

pub fn new(name: String) -> Store(a, b) {
  let table_name = name <> int.to_string(unique_int())
  let ets_name = ets_new(table_name)

  let set = fn(a, b) { ets_set(ets_name, a, b) }
  let insert = fn(a, b) { ets_insert(ets_name, a, b) }
  let lookup = fn(a) { ets_lookup(ets_name, a) }
  let delete = fn() { ets_delete(ets_name) }

  Store(set:, insert:, lookup:, delete:)
}

pub type StoreError {
  StoreError(message: String)
}

fn ets_set(name: Atom, key: a, with next: fn(b) -> b) -> Result(b, StoreError) {
  exception.rescue(fn() {
    ets_lookup(name, key)
    |> result.map(next)
    |> result.map(fn(val) {
      ets_insert(name, key, val)

      val
    })
  })
  |> result.unwrap(Error(StoreError("oh no")))
}

fn ets_new(name: String) -> Atom {
  atom.create(name) |> ets_new_
}

fn ets_insert(name: Atom, key: a, value: b) -> #(a, b) {
  ets_insert_(name, #(key, value))
}

fn ets_lookup(name: Atom, key: a) -> Result(b, StoreError) {
  ets_lookup_(name, key)
  |> dict.from_list
  |> dict.get(key)
  |> result.replace_error(StoreError("key not found"))
}

fn ets_delete(name: Atom) -> Nil {
  ets_delete_(name)

  Nil
}

@external(erlang, "pgl_ffi", "unique_int")
fn unique_int() -> Int

@external(erlang, "pgl_ffi", "ets_new")
fn ets_new_(name: Atom) -> Atom

@external(erlang, "ets", "insert")
fn ets_insert_(name: Atom, key_val: #(a, b)) -> #(a, b)

@external(erlang, "ets", "lookup")
fn ets_lookup_(module: Atom, key: a) -> List(#(a, b))

@external(erlang, "ets", "delete")
fn ets_delete_(module: Atom) -> b
