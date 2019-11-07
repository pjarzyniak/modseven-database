<?php
/**
 * PDO database connection.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license    https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Driver;

use Modseven\Core;
use Modseven\Profiler;
use Modseven\Database\Database;
use Modseven\Database\Exception;

class PDO extends Database {

    /**
     * PDO uses no quoting for identifiers
     * @var string
     */
    protected $_identifier = '';

    /**
     * Stores the database configuration locally and name the instance.
     *
     * [!!] This method cannot be accessed directly, you must use [Database::instance].
     *
     * @param string $name   Instance name
     * @param array  $config Configuration
     */
    public function __construct(string $name, array $config)
    {
        parent::__construct($name, $config);

        if (isset($this->_config['identifier']))
        {
            // Allow the identifier to be overloaded per-connection
            $this->_identifier = (string)$this->_config['identifier'];
        }
    }

    /**
     * Connect to the database. This is called automatically when the first
     * query is executed.
     *
     * @return  void
     *
     * @throws  Exception
     * @throws \Modseven\Exception
     */
    public function connect() : void
    {
        if ($this->_connection)
        {
            return;
        }

        // Extract the connection parameters, adding required variables
        extract($this->_config['connection']+[
                'dsn' => '',
                'username' => NULL,
                'password' => NULL,
                'persistent' => FALSE,
            ], EXTR_OVERWRITE);

        // Clear the connection parameters for security
        unset($this->_config['connection']);

        // Force PDO to use exceptions for all errors
        $options[\PDO::ATTR_ERRMODE] = \PDO::ERRMODE_EXCEPTION;

        if ( ! empty($persistent))
        {
            // Make the connection persistent
            $options[\PDO::ATTR_PERSISTENT] = TRUE;
        }

        try
        {
            // Create a new PDO connection
            $this->_connection = new \PDO($dsn, $username, $password, $options);
        }
        catch (\PDOException $e)
        {
            throw new Exception(':error', [':error' => $e->getMessage()], $e->getCode());
        }

        if ( ! empty($this->_config['charset']))
        {
            // Set the character set
            $this->setCharset($this->_config['charset']);
        }
    }

    /**
     * Create or redefine a SQL aggregate function.
     * [!!] Works only with SQLite
     *
     * @param string   $name      Name of the SQL function to be created or redefined
     * @param callback $step      Called for each row of a result set
     * @param callback $final     Called after all rows of a result set have been processed
     * @param integer  $arguments Number of arguments that the SQL function takes
     *
     * @return  boolean
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function createAggregate(string $name, $step, $final, int $arguments = -1) : bool
    {
        $this->_connection or $this->connect();

        return $this->_connection->sqliteCreateAggregate($name, $step, $final, $arguments);
    }

    /**
     * Create or redefine a SQL function.
     *
     * [!!] Works only with SQLite
     *
     * @param string   $name      Name of the SQL function to be created or redefined
     * @param callback $callback  Callback which implements the SQL function
     * @param integer  $arguments Number of arguments that the SQL function takes
     *
     * @return  boolean
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function createFunction(string $name, $callback, int $arguments = -1) : bool
    {
        $this->_connection or $this->connect();

        return $this->_connection->sqliteCreateFunction($name, $callback, $arguments);
    }

    /**
     * Disconnect from PDO
     *
     * @return bool
     */
    public function disconnect() : bool
    {
        // Destroy the PDO object
        $this->_connection = NULL;

        return parent::disconnect();
    }

    /**
     * Set character set to use
     *
     * @param string $charset
     *
     * @throws Exception
     *
     * @throws \Modseven\Exception
     */
    public function setCharset(string $charset) : void
    {
        // Make sure the database is connected
        $this->_connection OR $this->connect();

        // This SQL-92 syntax is not supported by all drivers
        $this->_connection->exec('SET NAMES '.$this->quote($charset));
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
    public function query(int $type, string $sql, $as_object = FALSE, ?array $params = NULL)
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        if (Core::$profiling)
        {
            // Benchmark this query for the current instance
            $benchmark = Profiler::start("Database ({$this->_instance})", $sql);
        }

        try
        {
            $result = $this->_connection->query($sql);
        }
        catch (\Exception $e)
        {
            if (isset($benchmark))
            {
                // This benchmark is worthless
                Profiler::delete($benchmark);
            }

            // Convert the exception in a database exception
            throw new Exception(':error [ :query ]', [
                    ':error' => $e->getMessage(),
                    ':query' => $sql
                ], $e->getCode());
        }

        if (isset($benchmark))
        {
            Profiler::stop($benchmark);
        }

        // Set the last query
        $this->last_query = $sql;

        if ($type === Database::SELECT)
        {
            // Convert the result into an array, as PDOStatement::rowCount is not reliable
            if ($as_object === FALSE)
            {
                $result->setFetchMode(\PDO::FETCH_ASSOC);
            }
            elseif (is_string($as_object))
            {
                $result->setFetchMode(\PDO::FETCH_CLASS, $as_object, $params);
            }
            else
            {
                $result->setFetchMode(\PDO::FETCH_CLASS, 'stdClass');
            }

            $result = $result->fetchAll();

            // Return an iterator of results
            return new \Modseven\Database\Result\Cached($result, $sql, $as_object, $params);
        }

        if ($type === Database::INSERT)
        {
            // Return a list of insert id and rows created
            return [
                $this->_connection->lastInsertId(),
                $result->rowCount(),
            ];
        }

        // Return the number of rows affected
        return $result->rowCount();
    }

    /**
     * Start a SQL transaction
     *
     * @param string $mode transaction mode
     *
     * @throws Exception
     *
     * @return  boolean
     */
    public function begin(?string $mode = NULL) : bool
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        return $this->_connection->beginTransaction();
    }

    /**
     * Commit the current transaction
     *
     * @throws Exception
     *
     * @return  boolean
     */
    public function commit() : bool
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        return $this->_connection->commit();
    }

    /**
     * Abort the current transaction
     *
     * @throws Exception
     *
     * @return  boolean
     */
    public function rollback() : bool
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        return $this->_connection->rollBack();
    }

    /**
     * List all of the tables in the database. Optionally, a LIKE string can
     * be used to search for specific tables.
     *
     * @param string $like table to search for
     *
     * @throws Exception
     *
     * @return  array
     */
    public function listTables(?string $like = NULL) : array
    {
        throw new Exception('Database method :method is not supported by :class', [
                ':method' => __FUNCTION__,
                ':class' => __CLASS__
            ]);
    }

    /**
     * Lists all of the columns in a table. Optionally, a LIKE string can be
     * used to search for specific fields.
     *
     * @param string  $table      table to get columns from
     * @param string  $like       column to search for
     * @param boolean $add_prefix whether to add the table prefix automatically or not
     *
     * @throws Exception
     *
     * @return  array
     */
    public function listColumns($table, ?string $like = NULL, bool $add_prefix = TRUE) : array
    {
        throw new Exception('Database method :method is not supported by :class', [
                ':method' => __FUNCTION__,
                ':class' => __CLASS__
            ]);
    }

    /**
     * Sanitize a string by escaping characters that could cause an SQL
     * injection attack.
     *
     * @param string $value value to quote
     *
     * @throws Exception
     *
     * @return  string
     */
    public function escape(string $value) : string
    {
        // Make sure the database is connected
        $this->_connection or $this->connect();

        return $this->_connection->quote($value);
    }

}
