import gleam/erlang/process
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import rasa/table.{type Table}

pub opaque type QueryCache {
  QueryCache(name: process.Name(Message))
}

type Message {
  Lookup(client: process.Subject(Result(List(Int), Nil)), query: String)
  Insert(
    client: process.Subject(Result(Nil, Nil)),
    query: String,
    desc: List(Int),
  )
  Reset
  Delete(query: String)
  Shutdown
}

const query_cache_name = "pgl_query_cache"

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

    table.new()
    |> table.with_access(table.Private)
    |> table.build
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
) -> Result(List(Int), Nil) {
  process.named_subject(query_cache.name)
  |> actor.call(1000, Lookup(_, query))
}

pub fn insert(
  query_cache: QueryCache,
  query: String,
  oids: List(Int),
) -> Result(Nil, Nil) {
  process.named_subject(query_cache.name)
  |> actor.call(1000, Insert(_, query, oids))
}

pub fn reset(query_cache: QueryCache) -> Nil {
  process.named_subject(query_cache.name)
  |> actor.send(Reset)
}

pub fn delete(query_cache: QueryCache, query: String) -> Nil {
  process.named_subject(query_cache.name)
  |> actor.send(Delete(query))
}

pub fn shutdown(query_cache: QueryCache) -> Nil {
  process.named_subject(query_cache.name)
  |> process.send(Shutdown)
}

fn handle_message(
  table: Table(String, List(Int)),
  msg: Message,
) -> actor.Next(Table(String, List(Int)), Message) {
  case msg {
    Lookup(client, query) -> {
      table.lookup(table, query)
      |> actor.send(client, _)

      actor.continue(table)
    }
    Insert(client, query, description) -> {
      table.insert(table, query, description)
      |> result.replace(Nil)
      |> actor.send(client, _)

      actor.continue(table)
    }
    Reset -> {
      let _ = table.drop(table)

      table.new()
      |> table.with_access(table.Private)
      |> table.build
      |> actor.continue
    }
    Delete(query) -> {
      let _ = table.delete(table, query)

      actor.continue(table)
    }
    Shutdown -> {
      let _ = table.drop(table)

      actor.stop()
    }
  }
}
