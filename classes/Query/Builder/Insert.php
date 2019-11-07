<?php
/**
 * Database query builder for INSERT statements.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license    https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Query\Builder;

use Modseven\Database\Query;
use Modseven\Database\Database;
use Modseven\Database\Exception;
use Modseven\Database\Query\Builder;

class Insert extends Builder
{
    /**
     * Table to insert into
     * @var string
     */
    protected $_table;

    /**
     * Columns where to insert
     * @var array
     */
    protected $_columns = [];

    /**
     * Values to insert
     * @var array
     */
    protected $_values = [];

    /**
     * Set the table and columns for an insert.
     *
     * @param mixed $table   table name or array($table, $alias) or object
     * @param array $columns column names
     *
     * @throws Exception
     */
    public function __construct($table = NULL, array $columns = NULL)
    {
        if ($table)
        {
            // Set the initial table name
            $this->table($table);
        }

        if ($columns)
        {
            // Set the column names
            $this->_columns = $columns;
        }

        // Start the query with no SQL
        parent::__construct(Database::INSERT, '');
    }

    /**
     * Sets the table to insert into.
     *
     * @param string $table table name
     *
     * @throws Exception
     *
     * @return  self
     */
    public function table($table) : self
    {
        if ( ! is_string($table))
        {
            throw new Exception('INSERT INTO syntax does not allow table aliasing');
        }

        $this->_table = $table;

        return $this;
    }

    /**
     * Set the columns that will be inserted.
     *
     * @param array $columns column names
     *
     * @return  self
     */
    public function columns(array $columns) : self
    {
        $this->_columns = $columns;

        return $this;
    }

    /**
     * Adds or overwrites values. Multiple value sets can be added.
     *
     * @param array $values values list
     *
     * @throws Exception
     *
     * @return  self
     */
    public function values(array $values) : self
    {
        if ( ! is_array($this->_values))
        {
            throw new Exception('INSERT INTO ... SELECT statements cannot be combined with INSERT INTO ... VALUES');
        }

        foreach (func_get_args() as $value)
        {
            $this->_values[] = $value;
        }

        return $this;
    }

    /**
     * Use a sub-query to for the inserted values.
     *
     * @param Query $query Database_Query of SELECT type
     *
     * @throws Exception
     *
     * @return  self
     */
    public function select(Query $query) : self
    {
        if ($query->type() !== Database::SELECT)
        {
            throw new Exception('Only SELECT queries can be combined with INSERT queries');
        }

        $this->_values = $query;

        return $this;
    }

    /**
     * Compile the SQL query and return it.
     *
     * @param mixed $db Database instance or name of instance
     *
     * @return  string
     *
     * @throws Exception
     * @throws \Modseven\Exception
     */
    public function compile($db = NULL) : string
    {
        if ( ! is_object($db))
        {
            // Get the database instance
            $db = Database::instance($db);
        }

        // Start an insertion query
        $query = 'INSERT INTO '.$db->quoteTable($this->_table);

        // Add the column names
        $query .= ' ('.implode(', ', array_map([
                $db,
                'quoteColumn'
            ], $this->_columns)).') ';

        if (is_array($this->_values))
        {
            // Callback for quoting values

            $groups = [];
            foreach ($this->_values as $group)
            {
                foreach ($group as $offset => $value)
                {
                    if ((is_string($value) && array_key_exists($value, $this->_parameters)) === FALSE)
                    {
                        // Quote the value, it is not a parameter
                        $group[$offset] = $db->quote($value);
                    }
                }

                $groups[] = '('.implode(', ', $group).')';
            }

            // Add the values
            $query .= 'VALUES '.implode(', ', $groups);
        }
        else
        {
            // Add the sub-query
            $query .= $this->_values;
        }

        $this->_sql = $query;

        return parent::compile($db);
    }

    /**
     * Reset the query
     *
     * @return self
     */
    public function reset()
    {
        $this->_table = NULL;
        $this->_columns = $this->_values = [];
        $this->_parameters = [];
        $this->_sql = NULL;
        return $this;
    }

}
