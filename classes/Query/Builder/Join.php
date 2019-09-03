<?php
/**
 * Database query builder for JOIN statements.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Query\Builder;

use Modseven\Database\Database;
use Modseven\Database\Exception;

class Join extends \Modseven\Database\Query\Builder {

    /**
     * JOIN Type (LEFT, RIGHT, INNER, OUTER, etc..)
     * @var string
     */
    protected $_type;

    /**
     * Table to join
     * @var mixed
     */
    protected $_table;

    /**
     * Join "on"
     * @var array
     */
    protected $_on = [];

    /**
     * Join using
     * @var array
     */
    protected $_using = [];

    /**
     * Creates a new JOIN statement for a table. Optionally, the type of JOIN
     * can be specified as the second parameter.
     *
     * @param mixed  $table column name or array($column, $alias) or object
     * @param string $type  type of JOIN: INNER, RIGHT, LEFT, etc
     */
    public function __construct($table, $type = NULL)
    {
        // Set the table to JOIN on
        $this->_table = $table;

        if ($type !== NULL)
        {
            // Set the JOIN type
            $this->_type = (string)$type;
        }
    }

    /**
     * Adds a new condition for joining.
     *
     * @param mixed  $c1 column name or array($column, $alias) or object
     * @param string $op logic operator
     * @param mixed  $c2 column name or array($column, $alias) or object
     *
     * @throws Exception
     *
     * @return  self
     */
    public function on($c1, string $op, $c2) : self
    {
        if ( ! empty($this->_using))
        {
            throw new Exception('JOIN ... ON ... cannot be combined with JOIN ... USING ...');
        }

        $this->_on[] = [
            $c1,
            $op,
            $c2
        ];

        return $this;
    }

    /**
     * Adds a new condition for joining.
     *
     * @param string $columns column name
     *
     * @throws Exception
     *
     * @return  self
     */
    public function using($columns) : self
    {
        if ( ! empty($this->_on))
        {
            throw new Exception('JOIN ... ON ... cannot be combined with JOIN ... USING ...');
        }

        $this->_using = array_merge($this->_using, func_get_args());

        return $this;
    }

    /**
     * Compile the SQL partial for a JOIN statement and return it.
     *
     * @param mixed $db Database instance or name of instance
     *
     * @throws Exception
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

        if ($this->_type)
        {
            $sql = strtoupper($this->_type).' JOIN';
        }
        else
        {
            $sql = 'JOIN';
        }

        // Quote the table name that is being joined
        $sql .= ' '.$db->quote_table($this->_table);

        if ( ! empty($this->_using))
        {
            // Quote and concat the columns
            $sql .= ' USING ('.implode(', ', array_map([
                    $db,
                    'quote_column'
                ], $this->_using)).')';
        }
        else
        {
            $conditions = [];
            foreach ($this->_on as $condition)
            {
                // Split the condition
                [$c1, $op, $c2] = $condition;

                if ($op)
                {
                    // Make the operator uppercase and spaced
                    $op = ' '.strtoupper($op);
                }

                // Quote each of the columns used for the condition
                $conditions[] = $db->quote_column($c1).$op.' '.$db->quote_column($c2);
            }

            // Concat the conditions "... AND ..."
            $sql .= ' ON ('.implode(' AND ', $conditions).')';
        }

        return $sql;
    }

    /**
     * Reset Query
     *
     * @return self
     */
    public function reset() : self
    {
        $this->_type = $this->_table = NULL;
        $this->_on = [];
        return $this;
    }

}
