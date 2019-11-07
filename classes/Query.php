<?php
/**
 * Database query wrapper.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license    https://koseven.ga/LICENSE
 */

namespace Modseven\Database;

use Modseven\Core;
use Modseven\Database\Result\Cached;

class Query
{
    /**
     * Query type
     * @var int
     */
    protected $_type;

    /**
     * Execute the query during a cache hit
     * @var bool
     */
    protected $_force_execute = FALSE;

    /**
     * Cache lifetime
     * @var null
     */
    protected $_lifetime;

    /**
     * SQL statement
     * @var string
     */
    protected $_sql;

    /**
     * Quoted query parameters
     * @var array
     */
    protected $_parameters = [];

    /**
     * Return results as associative arrays or objects
     * @var bool
     */
    protected $_as_object = FALSE;

    /**
     * Parameters for __construct when using object results
     * @var array
     */
    protected $_object_params = [];

    /**
     * Creates a new SQL query of the specified type.
     *
     * @param integer $type query type: Database::SELECT, Database::INSERT, etc
     * @param string  $sql  query string
     */
    public function __construct(int $type, string $sql)
    {
        $this->_type = $type;
        $this->_sql = $sql;
    }

    /**
     * Return the SQL query string.
     *
     * @throws \Modseven\Exception
     *
     * @return  string
     */
    public function __toString() : string
    {
        try
        {
            // Return the SQL string
            return $this->compile(Database::instance());
        }
        catch (Exception $e)
        {
            return Exception::text($e);
        }
    }

    /**
     * Get the type of the query.
     *
     * @return  integer
     */
    public function type() : int
    {
        return $this->_type;
    }

    /**
     * Enables the query to be cached for a specified amount of time.
     *
     * @param integer $lifetime number of seconds to cache, 0 deletes it from the cache
     * @param boolean $force    whether or not to execute the query during a cache hit
     *
     * @return  self
     */
    public function cached(int $lifetime, ?bool $force = FALSE) : self
    {
        $this->_force_execute = $force;
        $this->_lifetime = $lifetime;

        return $this;
    }

    /**
     * Returns results as associative arrays
     *
     * @return  self
     */
    public function asAssoc() : self
    {
        $this->_as_object = FALSE;

        $this->_object_params = [];

        return $this;
    }

    /**
     * Returns results as objects
     *
     * @param mixed  $class classname or TRUE for stdClass
     * @param array  $params
     *
     * @return  self
     */
    public function asObject($class = TRUE, ?array $params = NULL) : self
    {
        $this->_as_object = $class;

        if ($params)
        {
            // Add object parameters
            $this->_object_params = $params;
        }

        return $this;
    }

    /**
     * Set the value of a parameter in the query.
     *
     * @param string $param parameter key to replace
     * @param mixed  $value value to use
     *
     * @return  self
     */
    public function param(string $param, $value) : self
    {
        // Add or overload a new parameter
        $this->_parameters[$param] = $value;

        return $this;
    }

    /**
     * Bind a variable to a parameter in the query.
     *
     * @param string $param parameter key to replace
     * @param mixed  $var   variable to use
     *
     * @return  self
     */
    public function bind(string $param, &$var) : self
    {
        // Bind a value to a variable
        $this->_parameters[$param] =& $var;

        return $this;
    }

    /**
     * Add multiple parameters to the query.
     *
     * @param array $params list of parameters
     *
     * @return  self
     */
    public function parameters(array $params) : self
    {
        // Merge the new parameters in
        $this->_parameters = $params+$this->_parameters;

        return $this;
    }

    /**
     * Compile the SQL query and return it. Replaces any parameters with their
     * given values.
     *
     * @param mixed $db Database instance or name of instance
     *
     * @throws \Modseven\Exception
     *
     * @return  string
     */
    public function compile($db = NULL) : string
    {
        if ( ! is_object($db))
        {
            // Get the database instance
            $db = Database::instance($db);
        }

        // Import the SQL locally
        $sql = $this->_sql;

        if ( ! empty($this->_parameters))
        {
            // Quote all of the values
            $values = array_map([
                $db,
                'quote'
            ], $this->_parameters);

            // Replace the values in the SQL
            $sql = strtr($sql, $values);
        }

        return $sql;
    }

    /**
     * Execute the current query on the given database.
     *
     * @param mixed $db            Database instance or name of instance
     * @param bool  $as_object     result object classname, TRUE for stdClass or FALSE for array
     * @param array $object_params result object constructor arguments
     *
     * @return  mixed   Database_Result for SELECT queries, the insert id for INSERT queries, number of affected rows for all other queries
     *
     * @throws \Modseven\Exception
     */
    public function execute($db = NULL, ?bool $as_object = NULL, ?array $object_params = NULL)
    {
        if ( ! is_object($db))
        {
            // Get the database instance
            $db = Database::instance($db);
        }

        if ($as_object === NULL)
        {
            $as_object = $this->_as_object;
        }

        if ($object_params === NULL)
        {
            $object_params = $this->_object_params;
        }

        // Compile the SQL query
        $sql = $this->compile($db);

        if ($this->_lifetime !== NULL && $this->_type === Database::SELECT)
        {
            // Set the cache key based on the database instance name and SQL
            $cache_key = '\Modseven\Database\Database::query("'.$db.'", "'.$sql.'")';

            // Read the cache first to delete a possible hit with lifetime <= 0
            if (! $this->_force_execute && ($result = Core::cache($cache_key, NULL, $this->_lifetime)) !== NULL)
            {
                // Return a cached result
                return new Cached($result, $sql, $as_object, $object_params);
            }
        }

        // Execute the query
        $result = $db->query($this->_type, $sql, $as_object, $object_params);

        if (isset($cache_key) && $this->_lifetime > 0)
        {
            // Cache the result array
            Core::cache($cache_key, $result->as_array(), $this->_lifetime);
        }

        return $result;
    }

}