import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import pgl/internal
import pgl/internal/store.{type Store}

const name = "pgl_query_cache"

pub opaque type QueryCache {
  QueryCache(np: process.Name(Message))
}

pub opaque type Message {
  Lookup(
    client: process.Subject(Result(List(Int), internal.PglError)),
    query: String,
  )
  Insert(client: process.Subject(Nil), query: String, desc: List(Int))
  Reset(client: process.Subject(Nil))
  Shutdown
}

// Table

const table_name = "pgl_query_cache_table"

pub fn new() -> QueryCache {
  process.new_name(name) |> QueryCache
}

pub fn supervised(qc: QueryCache) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start(qc) })
  |> supervision.restart(supervision.Transient)
}

pub fn start(qc: QueryCache) -> Result(actor.Started(Nil), actor.StartError) {
  actor.new_with_initialiser(1000, fn(subj) {
    let selector = process.new_selector() |> process.select(subj)

    store.new(table_name)
    |> actor.initialised
    |> actor.selecting(selector)
    |> Ok
  })
  |> actor.named(qc.np)
  |> actor.on_message(handle_message)
  |> actor.start
}

pub fn lookup(
  qc: QueryCache,
  query: String,
) -> Result(List(Int), internal.PglError) {
  process.named_subject(qc.np)
  |> actor.call(1000, Lookup(_, query))
}

pub fn insert(qc: QueryCache, query: String, oids: List(Int)) -> Nil {
  process.named_subject(qc.np)
  |> actor.call(1000, Insert(_, query, oids))
}

pub fn reset(qc: QueryCache) -> Nil {
  process.named_subject(qc.np)
  |> process.call(1000, Reset)
}

pub fn shutdown(qc: QueryCache) -> Nil {
  process.named_subject(qc.np) |> process.send(Shutdown)
}

fn handle_message(
  store: Store(String, List(Int)),
  msg: Message,
) -> actor.Next(Store(String, List(Int)), Message) {
  case msg {
    Lookup(client, query) -> {
      store.lookup(query)
      |> result.replace_error(internal.QueryCacheError(
        kind: internal.NotFoundError,
        message: "SQL query not found in cache",
      ))
      |> actor.send(client, _)

      actor.continue(store)
    }
    Insert(client, query, description) -> {
      store.insert(query, description)

      actor.send(client, Nil)
      actor.continue(store)
    }
    Reset(client) -> {
      store.delete()

      actor.send(client, Nil)
      actor.continue(store.new(table_name))
    }
    Shutdown -> {
      store.delete()

      actor.stop()
    }
  }
}
