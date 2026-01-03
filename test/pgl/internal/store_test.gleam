import pgl/internal/store

pub fn insert_test() {
  let store = store.new("store_test")

  let assert Ok(Nil) = store.insert(store, "key", 10)
}

pub fn lookup_test() {
  let store = store.new("store_test")

  let assert Ok(_) = store.insert(store, "key", 10)

  let assert Ok(10) = store.lookup(store, "key")
}

pub fn delete_test() {
  let store = store.new("store_test")

  let assert Ok(_) = store.insert(store, "key", 10)

  let assert Ok(10) = store.lookup(store, "key")

  let assert Ok(Nil) = store.delete(store, "key")

  let assert Error(Nil) = store.lookup(store, "key")
}

pub fn drop_test() {
  let store = store.new("store_test")

  let assert Ok(_) = store.insert(store, "key", 10)

  let assert Ok(10) = store.lookup(store, "key")

  let assert Ok(Nil) = store.drop(store)

  let assert Error(Nil) = store.lookup(store, "key")
}
