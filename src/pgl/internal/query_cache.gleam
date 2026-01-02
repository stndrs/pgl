import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import pgl/internal
import pgl/internal/store.{type Store}

pub opaque type QueryCache {
  QueryCache(name: process.Name(Message))
}

type Message {
  Lookup(
    client: process.Subject(Result(List(Int), internal.InternalError)),
    query: String,
  )
  Insert(client: process.Subject(Nil), query: String, desc: List(Int))
  Reset(client: process.Subject(Nil))
  Shutdown
}

const query_cache_name = "pgl_query_cache"

const table_name = "pgl_query_cache_table"

pub fn new() -> QueryCache {
  query_cache_name
  |> process.new_name
  |> QueryCache
}

pub fn supervised(
  query_cache: QueryCache,
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start(query_cache) })
  |> supervision.restart(supervision.Transient)
}

pub fn start(
  query_cache: QueryCache,
) -> Result(actor.Started(Nil), actor.StartError) {
  actor.new_with_initialiser(1000, fn(subj) {
    let selector = process.new_selector() |> process.select(subj)

    store.new(table_name)
    |> actor.initialised
    |> actor.selecting(selector)
    |> Ok
  })
  |> actor.named(query_cache.name)
  |> actor.on_message(handle_message)
  |> actor.start
}

pub fn lookup(
  query_cache: QueryCache,
  query: String,
) -> Result(List(Int), internal.InternalError) {
  process.named_subject(query_cache.name)
  |> actor.call(1000, Lookup(_, query))
}

pub fn insert(query_cache: QueryCache, query: String, oids: List(Int)) -> Nil {
  process.named_subject(query_cache.name)
  |> actor.call(1000, Insert(_, query, oids))
}

pub fn reset(query_cache: QueryCache) -> Nil {
  process.named_subject(query_cache.name)
  |> actor.call(1000, Reset)
}

pub fn shutdown(query_cache: QueryCache) -> Nil {
  process.named_subject(query_cache.name) |> process.send(Shutdown)
}

fn handle_message(
  store: Store(String, List(Int)),
  msg: Message,
) -> actor.Next(Store(String, List(Int)), Message) {
  case msg {
    Lookup(client, query) -> {
      store.lookup(store, query)
      |> result.replace_error(internal.InternalError(
        "SQL query not found in cache",
      ))
      |> actor.send(client, _)

      actor.continue(store)
    }
    Insert(client, query, description) -> {
      store.insert(store, query, description)

      actor.send(client, Nil)
      actor.continue(store)
    }
    Reset(client) -> {
      store.delete(store)

      actor.send(client, Nil)
      actor.continue(store.new(table_name))
    }
    Shutdown -> {
      store.delete(store)

      actor.stop()
    }
  }
}
