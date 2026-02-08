//// PostgreSQL notification client

// notification system
// This notification system is very loosely based on the `pgo_notifications` module.
// Requirements while building:
// 1. ~~Use paramaterised queries for LISTEN and UNLISTEN commands.~~
//   - so turns out you can't use query parameters for the channel part of a LISTEN statement.
// 2. Reuse existing code for sending extended queries to database.
// This leads to a problem:
// - We need direct access to the socket for sending these subscribe messages.
// - A process needs to be constantly reading from this socket in order to receive
// notifications.
// - We don't have a good way to interrupt a read in gleam.
// - My initial idea, having two processes reading and writing messages on the the socket,
// then communicating with a manager process doesn't work, as it violates requirement 2.
// To solve this we have the following architecture:
// - One manager process which keeps track of which channels we're subscribed to and
// who needs to be notified on the arrival of a notification.
// - One reader process which constantly reads on the socket and sends incoming notifications
// to the manager process, which then forwards them on.
// - When the manager needs exclusive access to the socket for sending LISTEN and UNLISTEN
// commands, it writes a `Sync` message to the socket. This results in a `ReadyForQuery` message
// arriving at the reader, which uses this as a signal to quit reading.
//
// Notes:
// - If the manager crashes, we lose all state of what processes have subscribed to
// which channels and which channels we're even subscribed to.
// - For this reason, both processes are marked as `significant` causing the 
// supervisor to crash when they crash. A crash leaves all listening processes
// in a broken state, since they believe they're still subscribed, but they're not,
// they should crash by either linking to the manager process using `manager_pid`
// or being in a supervisor which gets restarted when the notification supervisor restarts.

import gleam/dict
import gleam/erlang/process
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/static_supervisor
import gleam/otp/supervision
import gleam/result
import gleam/string
import pgl
import pgl/internal
import pgl/internal/conn
import pgl/internal/encode
import pgl/internal/protocol
import pgl/internal/socket

/// A configured `Notifications` client. Must be started via
/// `notifications.start` or `notifications.supervised`.
pub opaque type Notifications {
  Notifications(
    manager: process.Name(ManagerMessage),
    reader: process.Name(ReaderMessage),
    db: pgl.Db,
  )
}

pub fn new(db: pgl.Db) -> Notifications {
  Notifications(
    manager: process.new_name("pgl_notifications_manager"),
    reader: process.new_name("pgl_notifications_reader"),
    db:,
  )
}

pub fn start(
  notifications: Notifications,
) -> actor.StartResult(static_supervisor.Supervisor) {
  let reader_subject = process.named_subject(notifications.reader)

  static_supervisor.new(static_supervisor.OneForAll)
  |> static_supervisor.add(supervised_reader(notifications.reader))
  |> static_supervisor.add(supervised_manager(
    notifications.manager,
    notifications.db,
    reader_subject,
  ))
  |> static_supervisor.restart_tolerance(0, 1)
  |> static_supervisor.start
}

pub fn manager_pid(notifications: Notifications) -> option.Option(process.Pid) {
  process.named(notifications.manager) |> option.from_result
}

pub fn supervised(
  notifications: Notifications,
) -> supervision.ChildSpecification(static_supervisor.Supervisor) {
  supervision.supervisor(fn() { start(notifications) })
}

pub fn listen(
  notifications: Notifications,
  channel: String,
  receiver: process.Subject(Notification),
) -> NotificationHandle {
  let manager = process.named_subject(notifications.manager)

  let assert Ok(handle) =
    actor.call(manager, 1000, fn(reply) { Listen(reply:, receiver:, channel:) })

  handle
}

pub fn unlisten(notifications: Notifications, handle: NotificationHandle) {
  let manager = process.named_subject(notifications.manager)
  actor.send(manager, Unlisten(handle))
}

pub opaque type NotificationHandle {
  NotificationHandle(monitor: process.Monitor)
}

type ManagerMessage {
  Listen(
    reply: process.Subject(Result(NotificationHandle, Nil)),
    receiver: process.Subject(Notification),
    channel: String,
  )
  Unlisten(handle: NotificationHandle)
  ReceivedNotification(notification: ReaderNotification)
}

type ManagerSubscribingState {
  ManagerIdle
  ManagerSubscribing(channel: String)
  ManagerUnsubscribing(channel: String)
}

type ManagerState {
  NotificationManagerState(
    inner_state: ManagerSubscribingState,
    db: pgl.Db,
    sock: socket.Socket,
    listeners: dict.Dict(
      String,
      List(#(process.Monitor, process.Subject(Notification))),
    ),
    // When we're LISTENing or UNLISTENing we still need to receive messages,
    // we requeue the messages received during the query for later.
    self_subject: process.Subject(ManagerMessage),
    reader: process.Subject(ReaderMessage),
    reader_receiver: process.Subject(ReaderNotification),
  )
}

fn start_manager(
  name: process.Name(ManagerMessage),
  db: pgl.Db,
  reader: process.Subject(ReaderMessage),
) -> actor.StartResult(Nil) {
  actor.new_with_initialiser(1000, fn(subj) {
    let reader_receiver = process.new_subject()

    let selector =
      process.new_selector()
      |> process.select(subj)
      |> process.select_map(reader_receiver, fn(reader) {
        ReceivedNotification(reader)
      })
      |> process.select_monitors(fn(down) {
        Unlisten(NotificationHandle(down.monitor))
      })

    use sock <- result.try(
      pgl.create_socket(db, socket.Infinite)
      |> result.replace_error("unable to create socket"),
    )

    start_reading(reader, sock, reader_receiver)

    NotificationManagerState(
      inner_state: ManagerIdle,
      db:,
      sock:,
      listeners: dict.new(),
      self_subject: subj,
      reader:,
      reader_receiver:,
    )
    |> actor.initialised
    |> actor.selecting(selector)
    |> Ok
  })
  |> actor.named(name)
  |> actor.on_message(handle_manager_message)
  |> actor.start
}

fn supervised_manager(
  name: process.Name(ManagerMessage),
  db: pgl.Db,
  reader: process.Subject(ReaderMessage),
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start_manager(name, db, reader) })
  |> supervision.timeout(1000)
  |> supervision.restart(supervision.Transient)
}

fn handle_manager_message(
  state: ManagerState,
  message: ManagerMessage,
) -> actor.Next(ManagerState, ManagerMessage) {
  case message {
    ReceivedNotification(ReaderNotification(notification)) -> {
      let receivers =
        dict.get(state.listeners, notification.channel) |> result.unwrap([])
      list.each(receivers, fn(receiver) {
        let #(_, receiver) = receiver
        process.send(receiver, notification)
      })
      actor.continue(state)
    }
    ReceivedNotification(StoppedReading) ->
      case state.inner_state {
        ManagerSubscribing(channel) -> {
          case subscribe(state, channel) {
            Ok(Nil) -> {
              start_reading(state.reader, state.sock, state.reader_receiver)
              actor.continue(
                NotificationManagerState(..state, inner_state: ManagerIdle),
              )
            }
            Error(error) ->
              actor.stop_abnormal(
                "failure to subscribe to channel "
                <> channel
                <> " "
                <> string.inspect(error),
              )
          }
        }
        ManagerUnsubscribing(channel) -> {
          case unsubscribe(state, channel) {
            Ok(Nil) -> {
              start_reading(state.reader, state.sock, state.reader_receiver)
              actor.continue(
                NotificationManagerState(..state, inner_state: ManagerIdle),
              )
            }
            Error(error) ->
              actor.stop_abnormal(
                "failure to unsubscribe from channel "
                <> channel
                <> " "
                <> string.inspect(error),
              )
          }
        }
        // StoppedReading should only be received, if we issued a sync before.
        ManagerIdle ->
          actor.stop_abnormal(
            "unexpected message ReceivedNotification(StoppedReading)",
          )
      }
    Listen(reply, receiver, channel) ->
      case state.inner_state {
        ManagerIdle -> {
          case
            result.map(process.subject_owner(receiver), fn(pid) {
              process.monitor(pid)
            })
          {
            Ok(monitor) -> {
              let channel_listeners = dict.get(state.listeners, channel)

              process.send(reply, Ok(NotificationHandle(monitor)))

              case channel_listeners {
                Error(Nil) ->
                  stop_reading(
                    NotificationManagerState(
                      ..state,
                      inner_state: ManagerSubscribing(channel),
                      listeners: dict.insert(state.listeners, channel, [
                        #(monitor, receiver),
                      ]),
                    ),
                    state.sock,
                  )
                Ok(channel_listeners) ->
                  actor.continue(
                    NotificationManagerState(
                      ..state,
                      listeners: dict.insert(state.listeners, channel, [
                        #(monitor, receiver),
                        ..channel_listeners
                      ]),
                    ),
                  )
              }
            }
            Error(Nil) -> {
              process.send(reply, Error(Nil))
              actor.continue(state)
            }
          }
        }
        _ -> {
          requeue_message(state, message)
        }
      }
    Unlisten(handle) ->
      case state.inner_state {
        ManagerIdle -> {
          process.demonitor_process(handle.monitor)

          case
            dict.fold(state.listeners, option.None, fn(acc, channel, listeners) {
              case acc {
                option.Some(_) -> acc
                option.None ->
                  case list.any(listeners, fn(v) { v.0 == handle.monitor }) {
                    True -> option.Some(channel)
                    False -> acc
                  }
              }
            })
          {
            option.Some(channel) -> {
              let assert Ok(channel_listeners) =
                dict.get(state.listeners, channel)

              case channel_listeners {
                // We need to unsubscribe from this channel
                [_] ->
                  stop_reading(
                    NotificationManagerState(
                      ..state,
                      listeners: dict.delete(state.listeners, channel),
                      inner_state: ManagerUnsubscribing(channel),
                    ),
                    state.sock,
                  )
                _ -> {
                  let channel_listeners =
                    list.filter(channel_listeners, fn(channel_listener) {
                      channel_listener.0 != handle.monitor
                    })
                  actor.continue(
                    NotificationManagerState(
                      ..state,
                      listeners: dict.insert(
                        state.listeners,
                        channel,
                        channel_listeners,
                      ),
                    ),
                  )
                }
              }
            }
            option.None -> actor.continue(state)
          }
        }
        _ -> {
          requeue_message(state, message)
        }
      }
  }
}

fn requeue_message(
  state: ManagerState,
  message: ManagerMessage,
) -> actor.Next(ManagerState, ManagerMessage) {
  process.send(state.self_subject, message)
  actor.continue(state)
}

// The reader process stops reading, when the servers sends the
// `ReadyForQuery` response due to our `Sync` command.
fn stop_reading(
  new_state: ManagerState,
  sock: socket.Socket,
) -> actor.Next(ManagerState, ManagerMessage) {
  case socket.send(sock, encode.sync()) |> result.replace(Nil) {
    Ok(Nil) -> {
      actor.continue(new_state)
    }
    Error(error) -> {
      actor.stop_abnormal(
        "error while writing sync to socket " <> string.inspect(error),
      )
    }
  }
}

fn escape_channel(raw_channel: String) -> String {
  "\"" <> string.replace(raw_channel, "\"", "\\\"") <> "\""
}

fn subscribe(
  state: ManagerState,
  channel: String,
) -> Result(Nil, internal.InternalError) {
  let connection = conn.new(state.sock, process.self())
  pgl.extended_query(
    "LISTEN " <> escape_channel(channel),
    [],
    connection,
    state.db,
    // Forwarding any notifications received during the query
    // to the manager for later processing.
    option.Some(fn(_proc_id, channel, payload) {
      process.send(
        state.self_subject,
        ReceivedNotification(
          ReaderNotification(Notification(channel:, payload:)),
        ),
      )
    }),
  )
  |> result.replace(Nil)
}

fn unsubscribe(
  state: ManagerState,
  channel: String,
) -> Result(Nil, internal.InternalError) {
  let connection = conn.new(state.sock, process.self())
  pgl.extended_query(
    "UNLISTEN " <> escape_channel(channel),
    [],
    connection,
    state.db,
    // Forwarding any notifications received during the query
    // to the manager for later processing.
    option.Some(fn(_proc_id, channel, payload) {
      process.send(
        state.self_subject,
        ReceivedNotification(
          ReaderNotification(Notification(channel:, payload:)),
        ),
      )
    }),
  )
  |> result.replace(Nil)
}

pub type Notification {
  Notification(channel: String, payload: String)
}

type ReaderNotification {
  ReaderNotification(notification: Notification)
  StoppedReading
}

type ReaderMessage {
  Read(socket: socket.Socket, receiver: process.Subject(ReaderNotification))
}

fn start_reading(
  reader: process.Subject(ReaderMessage),
  socket: socket.Socket,
  receiver: process.Subject(ReaderNotification),
) {
  process.send(reader, Read(socket:, receiver:))
}

fn start_reader(
  name: process.Name(ReaderMessage),
) -> actor.StartResult(process.Subject(ReaderMessage)) {
  actor.new(Nil)
  |> actor.named(name)
  |> actor.on_message(handle_reader_message)
  |> actor.start
}

fn supervised_reader(
  name: process.Name(ReaderMessage),
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() { start_reader(name) })
  |> supervision.timeout(1000)
  |> supervision.restart(supervision.Transient)
  |> supervision.map_data(fn(_) { Nil })
}

fn handle_reader_message(
  _state: Nil,
  message: ReaderMessage,
) -> actor.Next(Nil, ReaderMessage) {
  reader_receive_loop(message.socket, message.receiver)
}

fn reader_receive_loop(
  socket: socket.Socket,
  receiver: process.Subject(ReaderNotification),
) -> actor.Next(Nil, ReaderMessage) {
  case protocol.receive_message(socket) {
    Ok(message) ->
      case message {
        // When we want to subscribe to a channel, we don't want
        // the reader process to be active. By sending a `Sync`
        // message the postgres server sends this response to the
        // reader.
        internal.ReadyForQuery(..) -> {
          process.send(receiver, StoppedReading)
          actor.continue(Nil)
        }
        internal.NotificationResponse(_proc_id, channel, payload) -> {
          process.send(
            receiver,
            ReaderNotification(Notification(channel:, payload:)),
          )
          reader_receive_loop(socket, receiver)
        }
        internal.NoticeResponse(..) | internal.ParameterStatus(..) ->
          reader_receive_loop(socket, receiver)
        other ->
          actor.stop_abnormal(
            "Reader received unexpected message " <> string.inspect(other),
          )
      }
    Error(error) ->
      actor.stop_abnormal(
        "Reader failed to read from socket " <> string.inspect(error),
      )
  }
}
