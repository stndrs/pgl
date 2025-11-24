import pgl/config

pub fn set_database_test() {
  let conf = config.default
  let result = config.set_database(conf, "test_db")

  let assert "test_db" = result.database
  let assert True = result.host == conf.host
  let assert True = result.port == conf.port
}

pub fn set_host_test() {
  let conf = config.default
  let result = config.set_host(conf, "192.168.1.1")

  let assert "192.168.1.1" = result.host
  let assert True = result.database == conf.database
  let assert True = result.port == conf.port
}

pub fn set_port_test() {
  let conf = config.default
  let result = config.set_port(conf, 3306)

  let assert 3306 = result.port
  let assert True = result.host == conf.host
  let assert True = result.database == conf.database
}

pub fn set_username_test() {
  let conf = config.default
  let result = config.set_username(conf, "admin")

  let assert "admin" = result.user
  let assert True = result.host == conf.host
  let assert True = result.password == conf.password
}

pub fn set_password_test() {
  let conf = config.default
  let result = config.set_password(conf, "secret123")

  let assert "secret123" = result.password
  let assert True = result.user == conf.user
  let assert True = result.host == conf.host
}

pub fn set_ping_timeout_test() {
  let conf = config.default
  let result = config.set_ping_timeout(conf, 2000)

  let assert 2000 = result.ping_timeout
  let assert True = result.timeout == conf.timeout
  let assert True = result.send_timeout == conf.send_timeout
}

pub fn set_ssl_test() {
  let conf = config.default
  let result = config.set_ssl(conf, config.SslVerified)

  let assert config.SslVerified = result.ssl
  let assert True = result.host == conf.host
  let assert True = result.port == conf.port
}

pub fn default_values_test() {
  let conf = config.default

  let assert "127.0.0.1" = conf.host
  let assert 5432 = conf.port
  let assert "" = conf.user
  let assert "" = conf.password
  let assert "" = conf.database
  let assert 5000 = conf.timeout
  let assert 1000 = conf.ping_timeout
  let assert 5000 = conf.send_timeout
  let assert 5000 = conf.recv_timeout
  let assert config.SslDisabled = conf.ssl
}

pub fn default_port_constant_test() {
  let assert 5432 = config.default_port
}
