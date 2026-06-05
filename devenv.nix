{
  pkgs,
  config,
  ...
}:

let
  sslDir = "${config.env.DEVENV_STATE}/postgres-ssl";
  certFile = "${sslDir}/server.crt";
  keyFile = "${sslDir}/server.key";
in
{
  packages = [
    pkgs.openssl
  ];

  languages = {
    gleam.enable = true;
    erlang.enable = true;
  };

  # PGHOST and PGPORT are set automatically by services.postgres
  env = {
    PGUSER = "postgres";
    PGPASSWORD = "postgres";
    PGDATABASE = "pgl_test";
  };

  services.postgres = {
    enable = true;
    package = pkgs.postgresql_18;
    listen_addresses = "127.0.0.1,::1";
    port = 5432;

    initdbArgs = [
      "--locale=C"
      "--encoding=UTF8"
      "--username=postgres"
    ];

    initialDatabases = [
      { name = "pgl_test"; }
    ];

    initialScript = ''
      CREATE USER cleartext_user WITH PASSWORD 'cleartext_pass';
      GRANT ALL PRIVILEGES ON DATABASE pgl_test TO cleartext_user;
      CREATE USER trust_user;
      GRANT ALL PRIVILEGES ON DATABASE pgl_test TO trust_user;
      SET password_encryption = 'md5';
      CREATE USER md5_user WITH PASSWORD 'md5_pass';
      SET password_encryption = 'scram-sha-256';
      GRANT ALL PRIVILEGES ON DATABASE pgl_test TO md5_user;
      ALTER USER postgres WITH PASSWORD 'postgres';
    '';

    hbaConf = ''
      # TYPE  DATABASE        USER            ADDRESS                 METHOD
      local   all             all                                     trust
      host    all             cleartext_user  all                     password
      host    all             trust_user      all                     trust
      host    all             md5_user        all                     md5
      host    all             all             127.0.0.1/32            scram-sha-256
      host    all             all             ::1/128                 scram-sha-256
    '';

    settings = {
      log_statement = "all";
      ssl = "on";
      ssl_cert_file = certFile;
      ssl_key_file = keyFile;
    };
  };

  # Generate self-signed SSL certs before postgres starts
  processes.postgres-ssl-certs = {
    exec = ''
      set -euo pipefail
      mkdir -p ${sslDir}
      if [ ! -f ${certFile} ] || [ ! -f ${keyFile} ]; then
        ${pkgs.openssl}/bin/openssl req -new -x509 -days 365 -nodes -text \
          -out ${certFile} \
          -keyout ${keyFile} \
          -subj '/CN=localhost'
        chmod 600 ${keyFile}
        chmod 644 ${certFile}
        echo "SSL certificates generated."
      else
        echo "SSL certificates already exist, skipping generation."
      fi
    '';
    process-compose = {
      is_foreground = true;
    };
  };

  processes.postgres.process-compose = {
    depends_on.postgres-ssl-certs.condition = "process_completed_successfully";
    availability.restart = "no";
  };

  enterTest = ''
    echo "Running tests"
    pg_isready
  '';
}
