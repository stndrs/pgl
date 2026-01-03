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

pub fn insert(store: Store(a, b), key: a, value: b) -> Result(Nil, Nil) {
  ets_insert_(store.name, key, value)
}

pub fn lookup(store: Store(a, b), key: a) -> Result(b, Nil) {
  ets_lookup_(store.name, key)
}

pub fn delete(store: Store(a, b), key: a) -> Result(Nil, Nil) {
  ets_delete_key_(store.name, key)
}

pub fn drop(store: Store(a, b)) -> Result(Nil, Nil) {
  ets_delete_(store.name)
}

@external(erlang, "pgl_ffi", "unique_int")
fn unique_int() -> Int

@external(erlang, "pgl_ffi", "ets_new")
fn ets_new_(name: Atom) -> Atom

@external(erlang, "pgl_ffi", "ets_insert")
fn ets_insert_(name: Atom, key: a, val: b) -> Result(Nil, Nil)

@external(erlang, "pgl_ffi", "ets_lookup")
fn ets_lookup_(module: Atom, key: a) -> Result(b, Nil)

@external(erlang, "pgl_ffi", "ets_delete")
fn ets_delete_(module: Atom) -> Result(Nil, Nil)

@external(erlang, "pgl_ffi", "ets_delete")
fn ets_delete_key_(module: Atom, key: a) -> Result(Nil, Nil)
