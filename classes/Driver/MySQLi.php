<?php
/**
 * MySQLi database connection.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Driver;

use Modseven\Arr;
use Modseven\Database\MySQLi\Result;
use Modseven\Profiler;

use \Modseven\Database\Database;
use \Modseven\Database\Exception;

class MySQLi extends Database
{
    /**
     * Database in use by each connection
     * @var array
     */
    protected static $_current_databases = [];

    /**
     * SET the character set
     * @var bool
     */
    protected static $_set_names;

    /**
     * Identifier for this connection within the PHP driver
     * @var string
     */
    protected $_connection_id;

    /**
     * MySQL uses a backtick for identifiers
     * @var string
     */
    protected $_identifier = '`';

    /**
     * Connect to the database. This is called automatically when the first
     * query is executed.
     *
     * @return  void
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function connect() : void
    {
        if ($this->_connection)
        {
            return;
        }

        if (static::$_set_names === NULL)
        {
            // Determine if we can use mysqli_set_charset(), which is only
            // available on PHP 5.2.3+ when compiled against MySQL 5.0+
            static::$_set_names = ! function_exists('mysqli_set_charset');
        }

        // Extract the connection parameters, adding required variables
        extract($this->_config['connection']+[
                'database' => '',
                'hostname' => '',
                'username' => '',
                'password' => '',
                'socket' => '',
                'port' => 3306,
                'ssl' => NULL,
            ], EXTR_OVERWRITE);

        // Prevent this information from showing up in traces
        unset($this->_config['connection']['username'], $this->_config['connection']['password']);

        try
        {
            if (is_array($ssl))
            {
                $this->_connection = mysqli_init();
                $this->_connection->ssl_set(
                    Arr::get($ssl, 'client_key_path'),
                    Arr::get($ssl, 'client_cert_path'),
                    Arr::get($ssl, 'ca_cert_path'),
                    Arr::get($ssl, 'ca_dir_path'),
                    Arr::get($ssl, 'cipher')
                );
                $this->_connection->real_connect($hostname, $username, $password, $database, $port, $socket, MYSQLI_CLIENT_SSL);
            }
            else
            {
                $this->_connection = new \mysqli($hostname, $username, $password, $database, $port, $socket);
            }
        }
        catch (\Exception $e)
        {
            // No connection exists
            $this->_connection = NULL;

            throw new Exception(':error', [':error' => $e->getMessage()], $e->getCode());
        }

        // \xFF is a better delimiter, but the PHP driver uses underscore
        $this->_connection_id = sha1($hostname.'_'.$username.'_'.$password);

        if ( ! empty($this->_config['charset']))
        {
            // Set the character set
            $this->setCharset($this->_config['charset']);
        }

        if ( ! empty($this->_config['connection']['variables']))
        {
            // Set session variables
            $variables = [];

            foreach ($this->_config['connection']['variables'] as $var => $val)
            {
                $variables[] = 'SESSION '.$var.' = '.$this->quote($val);
            }

            $this->_connection->query('SET '.implode(', ', $variables));
        }
    }

    /**
     * Disconnect from the database. This is called automatically by [Database::__destruct].
     * Clears the database instance from [Database::$instances].
     *
     * @return  boolean
     */
    public function disconnect() : bool
    {
        try
        {
            // Database is assumed disconnected
            $status = TRUE;

            if (is_resource($this->_connection) && $status = $this->_connection->close())
            {
                // Clear the connection
                $this->_connection = NULL;

                // Clear the instance
                parent::disconnect();
            }
        }
        catch (\Exception $e)
        {
            // Database is probably not disconnected
            $status = ! is_resource($this->_connection);
        }

        return $status;
    }

    /**
     * Set the connection character set. This is called automatically by [Database::connect].
     *
     * @param string $charset character set name
     *
     * @return  void
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function setCharset(string $charset) : void
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        if (static::$_set_names === TRUE)
        {
            // PHP is compiled against MySQL 4.x
            $status = (bool)$this->_connection->query('SET NAMES '.$this->quote($charset));
        }
        else
        {
            // PHP is compiled against MySQL 5.x
            $status = $this->_connection->set_charset($charset);
        }

        if ($status === FALSE)
        {
            throw new Exception(':error', [':error' => $this->_connection->error], $this->_connection->errno);
        }
    }

    /**
     * Perform an SQL query of the given type.
     *
     * @param integer $type      Database::SELECT, Database::INSERT, etc
     * @param string  $sql       SQL query
     * @param mixed   $as_object result object class string, TRUE for stdClass, FALSE for assoc array
     * @param array   $params    object construct parameters for result class
     *
     * @return  mixed   Database_Result for SELECT queries, list (insert id, row count) for INSERT queries, number of affected rows for all other queries
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function query(int $type, string $sql, $as_object = FALSE, array $params = NULL)
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        if (\Modseven\Core::$profiling)
        {
            // Benchmark this query for the current instance
            $benchmark = Profiler::start("\Modseven\Database\Database ({$this->_instance})", $sql);
        }

        // Execute the query
        if (($result = $this->_connection->query($sql)) === FALSE)
        {
            if (isset($benchmark))
            {
                // This benchmark is worthless
                Profiler::delete($benchmark);
            }

            throw new Exception(':error [ :query ]', [
                ':error' => $this->_connection->error,
                ':query' => $sql
            ], $this->_connection->errno);
        }

        if (isset($benchmark))
        {
            Profiler::stop($benchmark);
        }

        // Set the last query
        $this->last_query = $sql;

        if ($type === Database::SELECT)
        {
            // Return an iterator of results
            return new Result($result, $sql, $as_object, $params);
        }
        if ($type === Database::INSERT)
        {
            // Return a list of insert id and rows created
            return [
                $this->_connection->insert_id,
                $this->_connection->affected_rows,
            ];
        }

        // Return the number of rows affected
        return $this->_connection->affected_rows;
    }

    /**
     * Returns a normalized array describing the SQL data type
     *
     * @param string $type SQL data type
     *
     * @return  array
     */
    public function datatype(string $type) : array
    {
        static $types = [
            'blob' => [
                'type' => 'string',
                'binary' => TRUE,
                'character_maximum_length' => '65535'
            ],
            'bool' => ['type' => 'bool'],
            'bigint unsigned' => [
                'type' => 'int',
                'min' => '0',
                'max' => '18446744073709551615'
            ],
            'datetime' => ['type' => 'string'],
            'decimal unsigned' => [
                'type' => 'float',
                'exact' => TRUE,
                'min' => '0'
            ],
            'double' => ['type' => 'float'],
            'double precision unsigned' => [
                'type' => 'float',
                'min' => '0'
            ],
            'double unsigned' => [
                'type' => 'float',
                'min' => '0'
            ],
            'enum' => ['type' => 'string'],
            'fixed' => [
                'type' => 'float',
                'exact' => TRUE
            ],
            'fixed unsigned' => [
                'type' => 'float',
                'exact' => TRUE,
                'min' => '0'
            ],
            'float unsigned' => [
                'type' => 'float',
                'min' => '0'
            ],
            'geometry' => [
                'type' => 'string',
                'binary' => TRUE
            ],
            'int unsigned' => [
                'type' => 'int',
                'min' => '0',
                'max' => '4294967295'
            ],
            'integer unsigned' => [
                'type' => 'int',
                'min' => '0',
                'max' => '4294967295'
            ],
            'longblob' => [
                'type' => 'string',
                'binary' => TRUE,
                'character_maximum_length' => '4294967295'
            ],
            'longtext' => [
                'type' => 'string',
                'character_maximum_length' => '4294967295'
            ],
            'json' => [
                'type' => 'string',
                'character_maximum_length' => '4294967295'
            ],
            'mediumblob' => [
                'type' => 'string',
                'binary' => TRUE,
                'character_maximum_length' => '16777215'
            ],
            'mediumint' => [
                'type' => 'int',
                'min' => '-8388608',
                'max' => '8388607'
            ],
            'mediumint unsigned' => [
                'type' => 'int',
                'min' => '0',
                'max' => '16777215'
            ],
            'mediumtext' => [
                'type' => 'string',
                'character_maximum_length' => '16777215'
            ],
            'national varchar' => ['type' => 'string'],
            'numeric unsigned' => [
                'type' => 'float',
                'exact' => TRUE,
                'min' => '0'
            ],
            'nvarchar' => ['type' => 'string'],
            'point' => [
                'type' => 'string',
                'binary' => TRUE
            ],
            'real unsigned' => [
                'type' => 'float',
                'min' => '0'
            ],
            'set' => ['type' => 'string'],
            'smallint unsigned' => [
                'type' => 'int',
                'min' => '0',
                'max' => '65535'
            ],
            'text' => [
                'type' => 'string',
                'character_maximum_length' => '65535'
            ],
            'tinyblob' => [
                'type' => 'string',
                'binary' => TRUE,
                'character_maximum_length' => '255'
            ],
            'tinyint' => [
                'type' => 'int',
                'min' => '-128',
                'max' => '127'
            ],
            'tinyint unsigned' => [
                'type' => 'int',
                'min' => '0',
                'max' => '255'
            ],
            'tinytext' => [
                'type' => 'string',
                'character_maximum_length' => '255'
            ],
            'year' => ['type' => 'string'],
        ];

        $type = str_replace(' zerofill', '', $type);

        return $types[$type] ?? parent::datatype($type);
    }

    /**
     * Start a SQL transaction
     *
     * @param string $mode transaction mode
     *
     * @return  boolean
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function begin(?string $mode = NULL) : bool
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        if ($mode && ! $this->_connection->query("SET TRANSACTION ISOLATION LEVEL $mode"))
        {
            throw new Exception(':error', [
                ':error' => $this->_connection->error
            ], $this->_connection->errno);
        }

        return (bool)$this->_connection->query('START TRANSACTION');
    }

    /**
     * Commit the current transaction
     *
     * @return  boolean
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function commit() : bool
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        return (bool)$this->_connection->query('COMMIT');
    }

    /**
     * Rollback a SQL transaction
     *
     * @return boolean
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function rollback() : bool
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        return (bool)$this->_connection->query('ROLLBACK');
    }

    /**
     * List all of the tables in the database. Optionally, a LIKE string can
     * be used to search for specific tables.
     *
     * @param string $like table to search for
     *
     * @return  array
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function listTables(?string $like = NULL) : array
    {
        if (is_string($like))
        {
            // Search for table names
            $result = $this->query(Database::SELECT, 'SHOW TABLES LIKE '.$this->quote($like), FALSE);
        }
        else
        {
            // Find all table names
            $result = $this->query(Database::SELECT, 'SHOW TABLES', FALSE);
        }

        $tables = [];
        foreach ($result as $row)
        {
            $tables[] = reset($row);
        }

        return $tables;
    }

    /**
     * Lists all of the columns in a table. Optionally, a LIKE string can be
     * used to search for specific fields.
     *
     * @param string  $table      table to get columns from
     * @param string  $like       column to search for
     * @param boolean $add_prefix whether to add the table prefix automatically or not
     *
     * @return  array
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function listColumns($table, ?string $like = NULL, bool $add_prefix = TRUE) : array
    {
        // Quote the table name
        $table = ($add_prefix === TRUE) ? $this->quoteTable($table) : $table;

        if (is_string($like))
        {
            // Search for column names
            $result = $this->query(Database::SELECT, 'SHOW FULL COLUMNS FROM '.$table.' LIKE '.$this->quote($like), FALSE);
        }
        else
        {
            // Find all column names
            $result = $this->query(Database::SELECT, 'SHOW FULL COLUMNS FROM '.$table, FALSE);
        }

        $count = 0;
        $columns = [];
        foreach ($result as $row)
        {
            [$type, $length] = $this->_parseType($row['Type']);

            $column = $this->datatype($type);

            $column['column_name'] = $row['Field'];
            $column['column_default'] = $row['Default'];
            $column['data_type'] = $type;
            $column['is_nullable'] = ($row['Null'] === 'YES');
            $column['ordinal_position'] = ++$count;

            switch ($column['type'])
            {
                case 'float':
                    if (isset($length))
                    {
                        [$column['numeric_precision'], $column['numeric_scale']] = explode(',', $length);
                    }
                    break;
                case 'int':
                    if (isset($length))
                    {
                        // MySQL attribute
                        $column['display'] = $length;
                    }
                    break;
                case 'string':
                    switch ($column['data_type'])
                    {
                        case 'binary':
                        case 'varchar':
                        case 'char':
                        case 'varbinary':
                            $column['character_maximum_length'] = $length;
                            break;
                        case 'text':
                        case 'tinytext':
                        case 'mediumtext':
                        case 'longtext':
                        case 'json':
                            $column['collation_name'] = $row['Collation'];
                            break;
                        case 'enum':
                        case 'set':
                            $column['collation_name'] = $row['Collation'];
                            $column['options'] = explode('\',\'', substr($length, 1, -1));
                            break;
                    }
                    break;
            }

            // MySQL attributes
            $column['comment'] = $row['Comment'];
            $column['extra'] = $row['Extra'];
            $column['key'] = $row['Key'];
            $column['privileges'] = $row['Privileges'];

            $columns[$row['Field']] = $column;
        }

        return $columns;
    }

    /**
     * Sanitize a string by escaping characters that could cause an SQL
     * injection attack.
     *
     * @param string $value value to quote
     *
     * @return  string
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function escape(string $value) : string
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        if (($value = $this->_connection->real_escape_string($value)) === FALSE)
        {
            throw new Exception(':error', [
                ':error' => $this->_connection->error,
            ], $this->_connection->errno);
        }

        // SQL standard is to use single-quotes for all values
        return "'$value'";
    }

}
