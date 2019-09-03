<?php
/**
 * Database connection wrapper/helper.
 *
 * You may get a database instance using `Database::instance('name')` where
 * name is the config group.
 *
 * This class provides connection instance management via Database Drivers, as
 * well as quoting, escaping and other related functions. Querys are done using
 * Database_Query and Database_Query_Builder objects, which can be easily
 * created using the DB helper class.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database;

use Modseven\Database\Exception;

abstract class Database {

    // Query types
    const SELECT = 1;

    const INSERT = 2;

    const UPDATE = 3;

    const DELETE = 4;

    /**
     * Default instance name
     * @var string
     */
    public static $default = 'default';

    /**
     * Database instances
     * @var array
     */
    public static $instances = [];

    /**
     * The last query executed
     * @var string
     */
    public $last_query;

    /**
     * Character that is used to quote identifiers
     * @var string
     */
    protected $_identifier = '"';

    /**
     * Instance name
     * @var string
     */
    protected $_instance;

    /**
     * Configuration array
     * @var array
     */
    protected $_config;

    /**
     * Holds the current connection
     * @var mixed
     */
    protected $_connection;

    /**
     * Get a singleton Database instance. If configuration is not specified,
     * it will be loaded from the database configuration file using the same
     * group as the name.
     *
     * @param string $name   instance name
     * @param array  $config configuration parameters
     *
     * @throws Exception
     * @throws \KO7\Exception
     *
     * @return  self
     */
    public static function instance(?string $name = NULL, ?array $config = NULL) : self
    {
        if ($name === NULL)
        {
            // Use the default instance name
            $name = static::$default;
        }

        if ( ! isset(static::$instances[$name]))
        {
            if ($config === NULL)
            {
                // Load the configuration for this database
                $config = \KO7\Core::$config->load('database')->$name;
            }

            if ( ! isset($config['driver']))
            {
                throw new Exception('Database type not defined in :name configuration', [':name' => $name]);
            }

            // Store the database instance
            static::$instances[$name] = new $config['driver']($name, $config);;
        }

        return static::$instances[$name];
    }

    /**
     * Stores the database configuration locally and name the instance.
     *
     * [!!] This method cannot be accessed directly, you must use [Database::instance].
     *
     * @param string $name   Instance name
     * @param array  $config Configuration
     */
    public function __construct($name, array $config)
    {
        // Set the instance name
        $this->_instance = $name;

        // Store the config locally
        $this->_config = $config;

        if (empty($this->_config['table_prefix']))
        {
            $this->_config['table_prefix'] = '';
        }
    }

    /**
     * Disconnect from the database when the object is destroyed.
     *
     * [!!] Calling `unset($db)` is not enough to destroy the database, as it
     * will still be stored in `Database::$instances`.
     *
     */
    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Returns the database instance name.
     *
     * @return  string
     */
    public function __toString() : string
    {
        return $this->_instance;
    }

    /**
     * Connect to the database. This is called automatically when the first
     * query is executed.
     *
     * @throws  Exception
     *
     * @return  void
     */
    abstract public function connect() : void;

    /**
     * Disconnect from the database. This is called automatically by [Database::__destruct].
     * Clears the database instance from [Database::$instances].
     *
     * @return  boolean
     */
    public function disconnect() : bool
    {
        unset(static::$instances[$this->_instance]);

        return TRUE;
    }

    /**
     * Set the connection character set. This is called automatically by [Database::connect].
     *
     * @param string $charset character set name
     *
     * @throws  Exception
     *
     * @return  void
     */
    abstract public function set_charset(string $charset) : void;

    /**
     * Perform an SQL query of the given type.
     *
     * @param integer $type      Database::SELECT, Database::INSERT, etc
     * @param string  $sql       SQL query
     * @param mixed   $as_object result object class string, TRUE for stdClass, FALSE for assoc array
     * @param array   $params    object construct parameters for result class
     *
     * @return  object   Database_Result for SELECT queries
     * @return  array    list (insert id, row count) for INSERT queries
     * @return  integer  number of affected rows for all other queries
     */
    abstract public function query(int $type, string $sql, $as_object = FALSE, array $params = NULL);

    /**
     * Start a SQL transaction
     *
     * @param string $mode transaction mode
     *
     * @return  boolean
     */
    abstract public function begin(?string $mode = NULL) : bool;

    /**
     * Commit the current transaction
     *
     * @return  boolean
     */
    abstract public function commit() : bool;

    /**
     * Abort the current transaction
     *
     * @return  boolean
     */
    abstract public function rollback() : bool;

    /**
     * Count the number of records in a table.
     *
     * @param mixed $table table name string or array(query, alias)
     *
     * @return  integer
     */
    public function count_records($table) : int
    {
        // Quote the table name
        $table = $this->quote_table($table);

        return $this->query(static::SELECT, 'SELECT COUNT(*) AS total_row_count FROM '.$table, FALSE)->get('total_row_count');
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
            // SQL-92
            'bit' => [
                'type' => 'string',
                'exact' => TRUE
            ],
            'bit varying' => ['type' => 'string'],
            'char' => [
                'type' => 'string',
                'exact' => TRUE
            ],
            'char varying' => ['type' => 'string'],
            'character' => [
                'type' => 'string',
                'exact' => TRUE
            ],
            'character varying' => ['type' => 'string'],
            'date' => ['type' => 'string'],
            'dec' => [
                'type' => 'float',
                'exact' => TRUE
            ],
            'decimal' => [
                'type' => 'float',
                'exact' => TRUE
            ],
            'double precision' => ['type' => 'float'],
            'float' => ['type' => 'float'],
            'int' => [
                'type' => 'int',
                'min' => '-2147483648',
                'max' => '2147483647'
            ],
            'integer' => [
                'type' => 'int',
                'min' => '-2147483648',
                'max' => '2147483647'
            ],
            'interval' => ['type' => 'string'],
            'national char' => [
                'type' => 'string',
                'exact' => TRUE
            ],
            'national char varying' => ['type' => 'string'],
            'national character' => [
                'type' => 'string',
                'exact' => TRUE
            ],
            'national character varying' => ['type' => 'string'],
            'nchar' => [
                'type' => 'string',
                'exact' => TRUE
            ],
            'nchar varying' => ['type' => 'string'],
            'numeric' => [
                'type' => 'float',
                'exact' => TRUE
            ],
            'real' => ['type' => 'float'],
            'smallint' => [
                'type' => 'int',
                'min' => '-32768',
                'max' => '32767'
            ],
            'time' => ['type' => 'string'],
            'time with time zone' => ['type' => 'string'],
            'timestamp' => ['type' => 'string'],
            'timestamp with time zone' => ['type' => 'string'],
            'varchar' => ['type' => 'string'],

            // SQL:1999
            'binary large object' => [
                'type' => 'string',
                'binary' => TRUE
            ],
            'blob' => [
                'type' => 'string',
                'binary' => TRUE
            ],
            'boolean' => ['type' => 'bool'],
            'char large object' => ['type' => 'string'],
            'character large object' => ['type' => 'string'],
            'clob' => ['type' => 'string'],
            'national character large object' => ['type' => 'string'],
            'nchar large object' => ['type' => 'string'],
            'nclob' => ['type' => 'string'],
            'time without time zone' => ['type' => 'string'],
            'timestamp without time zone' => ['type' => 'string'],

            // SQL:2003
            'bigint' => [
                'type' => 'int',
                'min' => '-9223372036854775808',
                'max' => '9223372036854775807'
            ],

            // SQL:2008
            'binary' => [
                'type' => 'string',
                'binary' => TRUE,
                'exact' => TRUE
            ],
            'binary varying' => [
                'type' => 'string',
                'binary' => TRUE
            ],
            'varbinary' => [
                'type' => 'string',
                'binary' => TRUE
            ],
        ];

        return $types[$type] ?? [];
    }

    /**
     * List all of the tables in the database. Optionally, a LIKE string can
     * be used to search for specific tables.
     *
     * @param string $like table to search for
     *
     * @return  array
     */
    abstract public function list_tables(?string $like = NULL) : array;

    /**
     * Lists all of the columns in a table. Optionally, a LIKE string can be
     * used to search for specific fields.
     *
     * @param string  $table      table to get columns from
     * @param string  $like       column to search for
     * @param boolean $add_prefix whether to add the table prefix automatically or not
     *
     * @return  array
     */
    abstract public function list_columns($table, ?string $like = NULL, bool $add_prefix = TRUE) : array;

    /**
     * Extracts the text between parentheses, if any.
     *
     * @param string $type
     *
     * @return  array   list containing the type and length, if any
     */
    protected function _parse_type(string $type) : array
    {
        if (($open = strpos($type, '(')) === FALSE)
        {
            // No length specified
            return [
                $type,
                NULL
            ];
        }

        // Closing parenthesis
        $close = strrpos($type, ')', $open);

        // Length without parentheses
        $length = substr($type, $open+1, $close-1-$open);

        // Type without the length
        $type = substr($type, 0, $open).substr($type, $close+1);

        return [
            $type,
            $length
        ];
    }

    /**
     * Return the table prefix defined in the current configuration.
     *
     * @return  string
     */
    public function table_prefix() : string
    {
        return $this->_config['table_prefix'];
    }

    /**
     * Quote a value for an SQL query.
     *
     * Objects passed to this function will be converted to strings.
     * [Database_Expression] objects will be compiled.
     * [Database_Query] objects will be compiled and converted to a sub-query.
     * [stdClass] objects will be serialized using the `serialize` method.
     * All other objects will be converted using the `__toString` method.
     *
     * @param mixed $value any value to quote
     *
     * @return  string
     */
    public function quote($value) : string
    {
        if ($value === NULL)
        {
            return 'NULL';
        }
        if ($value === TRUE)
        {
            return "'1'";
        }
        if ($value === FALSE)
        {
            return "'0'";
        }
        if (is_object($value))
        {
            if ($value instanceof Query)
            {
                // Create a sub-query
                return '('.$value->compile($this).')';
            }
            if ($value instanceof Expression)
            {
                // Compile the expression
                return $value->compile($this);
            }
            if ($value instanceof stdClass)
            {
                // Convert the object to a string
                // Object of class stdClass could not be converted to string
                return $this->quote(serialize($value));
            }

            // Convert the object to a string
            return $this->quote((string)$value);
        }
        if (is_array($value))
        {
            return '('.implode(', ', array_map([
                    $this,
                    __FUNCTION__
                ], $value)).')';
        }
        if (is_int($value))
        {
            return (int)$value;
        }
        if (is_float($value))
        {
            // Convert to non-locale aware float to prevent possible commas
            return sprintf('%F', $value);
        }

        return $this->escape($value);
    }

    /**
     * Quote a database column name and add the table prefix if needed.
     *
     * Objects passed to this function will be converted to strings.
     * [Database_Expression] objects will be compiled.
     * [Database_Query] objects will be compiled and converted to a sub-query.
     * All other objects will be converted using the `__toString` method.
     *
     * @param mixed $column column name or array(column, alias)
     *
     * @return  string
     */
    public function quote_column($column) : string
    {
        // Identifiers are escaped by repeating them
        $escaped_identifier = $this->_identifier.$this->_identifier;

        if (is_array($column))
        {
            [$column, $alias] = $column;
            $alias = str_replace($this->_identifier, $escaped_identifier, $alias);
        }

        if ($column instanceof Query)
        {
            // Create a sub-query
            $column = '('.$column->compile($this).')';
        }
        elseif ($column instanceof Expression)
        {
            // Compile the expression
            $column = $column->compile($this);
        }
        else
        {
            // Convert to a string
            $column = (string)$column;

            $column = str_replace($this->_identifier, $escaped_identifier, $column);

            if ($column === '*')
            {
                return $column;
            }
            if (strpos($column, '.') !== FALSE)
            {
                $parts = explode('.', $column);

                if ($prefix = $this->table_prefix())
                {
                    // Get the offset of the table name, 2nd-to-last part
                    $offset = count($parts)-2;

                    // Add the table prefix to the table name
                    $parts[$offset] = $prefix.$parts[$offset];
                }

                foreach ($parts as &$part)
                {
                    if ($part !== '*')
                    {
                        // Quote each of the parts
                        $part = $this->_identifier.$part.$this->_identifier;
                    }
                }

                unset($part);

                $column = implode('.', $parts);
            }
            else
            {
                $column = $this->_identifier.$column.$this->_identifier;
            }
        }

        if (isset($alias))
        {
            $column .= ' AS '.$this->_identifier.$alias.$this->_identifier;
        }

        return $column;
    }

    /**
     * Quote a database table name and adds the table prefix if needed.
     *
     * Objects passed to this function will be converted to strings.
     * [Database_Expression] objects will be compiled.
     * [Database_Query] objects will be compiled and converted to a sub-query.
     * All other objects will be converted using the `__toString` method.
     *
     * @param mixed $table table name or array(table, alias)
     *
     * @return  string
     */
    public function quote_table($table) : string
    {
        // Identifiers are escaped by repeating them
        $escaped_identifier = $this->_identifier.$this->_identifier;

        if (is_array($table))
        {
            [$table, $alias] = $table;
            $alias = str_replace($this->_identifier, $escaped_identifier, $alias);
        }

        if ($table instanceof Query)
        {
            // Create a sub-query
            $table = '('.$table->compile($this).')';
        }
        elseif ($table instanceof Expression)
        {
            // Compile the expression
            $table = $table->compile($this);
        }
        else
        {
            // Convert to a string
            $table = (string)$table;

            $table = str_replace($this->_identifier, $escaped_identifier, $table);

            if (strpos($table, '.') !== FALSE)
            {
                $parts = explode('.', $table);

                if ($prefix = $this->table_prefix())
                {
                    // Get the offset of the table name, last part
                    $offset = count($parts)-1;

                    // Add the table prefix to the table name
                    $parts[$offset] = $prefix.$parts[$offset];
                }

                foreach ($parts as & $part)
                {
                    // Quote each of the parts
                    $part = $this->_identifier.$part.$this->_identifier;
                }

                unset($part);

                $table = implode('.', $parts);
            }
            else
            {
                // Add the table prefix
                $table = $this->_identifier.$this->table_prefix().$table.$this->_identifier;
            }
        }

        if (isset($alias))
        {
            // Attach table prefix to alias
            $table .= ' AS '.$this->_identifier.$this->table_prefix().$alias.$this->_identifier;
        }

        return $table;
    }

    /**
     * Quote a database identifier
     *
     * Objects passed to this function will be converted to strings.
     * [Database_Expression] objects will be compiled.
     * [Database_Query] objects will be compiled and converted to a sub-query.
     * All other objects will be converted using the `__toString` method.
     *
     * @param mixed $value any identifier
     *
     * @return  string
     */
    public function quote_identifier($value) : string
    {
        // Identifiers are escaped by repeating them
        $escaped_identifier = $this->_identifier.$this->_identifier;

        if (is_array($value))
        {
            [$value, $alias] = $value;
            $alias = str_replace($this->_identifier, $escaped_identifier, $alias);
        }

        if ($value instanceof Query)
        {
            // Create a sub-query
            $value = '('.$value->compile($this).')';
        }
        elseif ($value instanceof Expression)
        {
            // Compile the expression
            $value = $value->compile($this);
        }
        else
        {
            // Convert to a string
            $value = (string)$value;

            $value = str_replace($this->_identifier, $escaped_identifier, $value);

            if (strpos($value, '.') !== FALSE)
            {
                $parts = explode('.', $value);

                foreach ($parts as &$part)
                {
                    // Quote each of the parts
                    $part = $this->_identifier.$part.$this->_identifier;
                }

                unset($part);

                $value = implode('.', $parts);
            }
            else
            {
                $value = $this->_identifier.$value.$this->_identifier;
            }
        }

        if (isset($alias))
        {
            $value .= ' AS '.$this->_identifier.$alias.$this->_identifier;
        }

        return $value;
    }

    /**
     * Sanitize a string by escaping characters that could cause an SQL
     * injection attack.
     *
     * @param string $value value to quote
     *
     * @return  string
     */
    abstract public function escape(string $value) : string;

}
