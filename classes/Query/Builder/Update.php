<?php
/**
 * Database query builder for UPDATE statements.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Query\Builder;

use Modseven\Database\Database;
use Modseven\Database\Exception;

class Update extends Where {

    /**
     * Table to update
     * @var mixed|null
     */
    protected $_table;

    /**
     * Values to set
     * @var array
     */
    protected $_set = [];

    /**
     * Set the table for a update.
     *
     * @param mixed $table table name or array($table, $alias) or object
     */
    public function __construct($table = NULL)
    {
        if ($table)
        {
            // Set the inital table name
            $this->_table = $table;
        }

        // Start the query with no SQL
        parent::__construct(Database::UPDATE, '');
    }

    /**
     * Sets the table to update.
     *
     * @param mixed $table table name or array($table, $alias) or object
     *
     * @return  self
     */
    public function table($table) : self
    {
        $this->_table = $table;

        return $this;
    }

    /**
     * Set the values to update with an associative array.
     *
     * @param array $pairs associative (column => value) list
     *
     * @return  self
     */
    public function set(array $pairs) : self
    {
        foreach ($pairs as $column => $value)
        {
            $this->_set[] = [
                $column,
                $value
            ];
        }

        return $this;
    }

    /**
     * Set the value of a single column.
     *
     * @param mixed $column table name or array($table, $alias) or object
     * @param mixed $value  column value
     *
     * @return  self
     */
    public function value($column, $value) : self
    {
        $this->_set[] = [
            $column,
            $value
        ];

        return $this;
    }

    /**
     * Compile the SQL query and return it.
     *
     * @param mixed $db Database instance or name of instance
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

        // Start an update query
        $query = 'UPDATE '.$db->quote_table($this->_table);

        // Add the columns to update
        $query .= ' SET '.$this->_compile_set($db, $this->_set);

        if ( ! empty($this->_where))
        {
            // Add selection conditions
            $query .= ' WHERE '.$this->_compile_conditions($db, $this->_where);
        }

        if ( ! empty($this->_order_by))
        {
            // Add sorting
            $query .= ' '.$this->_compile_order_by($db, $this->_order_by);
        }

        if ($this->_limit !== NULL)
        {
            // Add limiting
            $query .= ' LIMIT '.$this->_limit;
        }

        $this->_sql = $query;

        return parent::compile($db);
    }

    /**
     * Reset query
     * @return self
     */
    public function reset()
    {
        $this->_table = NULL;
        $this->_set = $this->_where = [];
        $this->_limit = NULL;
        $this->_parameters = [];
        $this->_sql = NULL;
        return $this;
    }

}
