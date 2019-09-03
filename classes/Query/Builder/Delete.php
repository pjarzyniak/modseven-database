<?php
/**
 * Database query builder for DELETE statements.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Query\Builder;

use Modseven\Database\Database;

class Delete extends Where {

    /**
     * Table to delete from
     * @var mixed|null
     */
    protected $_table;

    /**
     * Set the table for a delete.
     *
     * @param mixed $table table name or array($table, $alias) or object
     *
     * @return  void
     */
    public function __construct($table = NULL)
    {
        if ($table)
        {
            // Set the initial table name
            $this->_table = $table;
        }

        // Start the query with no SQL
        parent::__construct(Database::DELETE, '');
    }

    /**
     * Sets the table to delete from.
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

        // Start a deletion query
        $query = 'DELETE FROM '.$db->quote_table($this->_table);

        if ( ! empty($this->_where))
        {
            // Add deletion conditions
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
     * Reset Query
     *
     * @return self
     */
    public function reset()
    {
        $this->_table = NULL;
        $this->_where = [];
        $this->_parameters = [];
        $this->_sql = NULL;
        return $this;
    }

}
