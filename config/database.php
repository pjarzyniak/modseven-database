<?php

return [
	'default' => [
		'type'       =>  \Modseven\Database\Driver\MySQLi::class,
		'connection' => [
			/**
			 * MySQLi driver config information
			 *
			 * The following options are available for MySQLi:
			 *
			 * string   hostname     server hostname, or socket
			 * string   database     database name
			 * string   username     database username
			 * string   password     database password
			 * boolean  persistent   use persistent connections?
			 * array    ssl          ssl parameters as "key => value" pairs.
			 *                       Available keys: client_key_path, client_cert_path, ca_cert_path, ca_dir_path, cipher
			 * array    variables    system variables as "key => value" pairs
			 *
			 * Ports and sockets may be appended to the hostname.
			 *
			 * MySQLi driver config example:
			 *
			 */
			'hostname'   => 'localhost',
			'database'   => 'koseven',
			'username'   => FALSE,
			'password'   => FALSE,
			'persistent' => FALSE,
			'ssl'        => NULL,
		],
		'table_prefix' => '',
		'charset'      => 'utf8',
		'caching'      => FALSE,
	],
	'alternate' => [
		'type'       =>  \Modseven\Database\Driver\PDO::class,
		'connection' => [
			/**
			 * The following options are available for PDO:
			 *
			 * string   dsn         Data Source Name
			 * string   username    database username
			 * string   password    database password
			 * boolean  persistent  use persistent connections?
			 */
			'dsn'        => 'mysql:host=localhost;dbname=koseven',
			'username'   => 'root',
			'password'   => 'r00tdb',
			'persistent' => FALSE,
		],
		/**
		 * The following extra options are available for PDO:
		 *
		 * string   identifier  set the escaping identifier
		 */
		'table_prefix' => '',
		'charset'      => 'utf8',
		'caching'      => FALSE,
	],
];
