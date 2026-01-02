import gleam/dict
import gleam/erlang/atom.{type Atom}
import gleam/int

pub opaque type Store(a, b) {
  Store(name: Atom)
}

pub fn new(name: String) -> Store(a, b) {
  let table_name = name <> int.to_string(unique_int())
  let name = atom.create(table_name) |> ets_new_

  Store(name:)
}

pub fn insert(store: Store(a, b), key: a, value: b) -> #(a, b) {
  ets_insert_(store.name, #(key, value))
}

pub fn lookup(store: Store(a, b), key: a) -> Result(b, Nil) {
  ets_lookup_(store.name, key)
  |> dict.from_list
  |> dict.get(key)
}

pub fn delete(store: Store(a, b)) -> Nil {
  ets_delete_(store.name)

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
fn ets_delete_(module: Atom) -> Bool
