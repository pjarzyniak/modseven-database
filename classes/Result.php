<?php
/**
 * Database result wrapper.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license    https://koseven.ga/LICENSE
 */

namespace Modseven\Database;

use Iterator;
use Countable;
use ArrayAccess;
use SeekableIterator;
use Modseven\Database\Result\Cached;

abstract class Result implements Countable, Iterator, SeekableIterator, ArrayAccess
{
    /**
     * Executed SQL for this result
     * @var string
     */
    protected $_query;

    /**
     * Raw result resource
     * @var mixed
     */
    protected $_result;

    /**
     * Total number of rows
     * @var int
     */
    protected $_total_rows = 0;

    /**
     * Current row number
     * @var int
     */
    protected $_current_row = 0;

    /**
     * Return rows as an object or associative array
     * @var mixed
     */
    protected $_as_object;

    /**
     * Parameters for __construct when using object results
     * @var array|null
     */
    protected $_object_params;

    /**
     * Sets the total number of rows and stores the result locally.
     *
     * @param mixed  $result query result
     * @param string $sql    SQL query
     * @param mixed  $as_object
     * @param array  $params
     */
    public function __construct($result, string $sql, $as_object = FALSE, ?array $params = NULL)
    {
        // Store the result locally
        $this->_result = $result;

        // Store the SQL locally
        $this->_query = $sql;

        if (is_object($as_object))
        {
            // Get the object class name
            $as_object = get_class($as_object);
        }

        // Results as objects or associative arrays
        $this->_as_object = $as_object;

        if ($params)
        {
            // Object constructor params
            $this->_object_params = $params;
        }
    }

    /**
     * Result destruction cleans up all open result sets.
     */
    abstract public function __destruct();

    /**
     * Get a cached database result from the current result iterator.
     *
     * @return  Cached
     */
    public function cached() : Cached
    {
        return new Cached($this->asArray(), $this->_query, $this->_as_object);
    }

    /**
     * Return all of the rows in the result as an array.
     *
     * @param string $key   column for associative keys
     * @param string $value column for values
     *
     * @return  array
     */
    public function asArray(?string $key = NULL, ?string $value = NULL) : array
    {
        $results = [];

        if ($key === NULL && $value === NULL)
        {
            // Indexed rows

            foreach ($this as $row)
            {
                $results[] = $row;
            }
        }
        elseif ($key === NULL)
        {
            // Indexed columns

            if ($this->_as_object)
            {
                foreach ($this as $row)
                {
                    $results[] = $row->$value;
                }
            }
            else
            {
                foreach ($this as $row)
                {
                    $results[] = $row[$value];
                }
            }
        }
        elseif ($value === NULL)
        {
            // Associative rows

            if ($this->_as_object)
            {
                foreach ($this as $row)
                {
                    $results[$row->$key] = $row;
                }
            }
            else
            {
                foreach ($this as $row)
                {
                    $results[$row[$key]] = $row;
                }
            }
        }
        elseif ($this->_as_object)
        {
            foreach ($this as $row)
            {
                $results[$row->$key] = $row->$value;
            }
        }
        else
        {
            foreach ($this as $row)
            {
                $results[$row[$key]] = $row[$value];
            }
        }

        $this->rewind();

        return $results;
    }

    /**
     * Return the named column from the current row.
     *
     * @param string $name    column to get
     * @param mixed  $default default value if the column does not exist
     *
     * @return  mixed
     */
    public function get(string $name, $default = NULL)
    {
        $row = $this->current();

        if ($this->_as_object)
        {
            if (isset($row->$name))
            {
                return $row->$name;
            }
        }
        elseif (isset($row[$name]))
        {
            return $row[$name];
        }

        return $default;
    }

    /**
     * Implements [Countable::count], returns the total number of rows.
     *
     * @return  integer
     */
    public function count() : int
    {
        return $this->_total_rows;
    }

    /**
     * Implements [ArrayAccess::offsetExists], determines if row exists.
     *
     * @param int $offset
     *
     * @return  boolean
     */
    public function offsetExists($offset) : bool
    {
        return ($offset >= 0 AND $offset < $this->_total_rows);
    }

    /**
     * Implements [ArrayAccess::offsetGet], gets a given row.
     *
     * @param int $offset
     *
     * @return  mixed
     */
    public function offsetGet($offset)
    {
        return $this->current();
    }

    /**
     * Implements [ArrayAccess::offsetSet], throws an error.
     *
     * [!!] You cannot modify a database result.
     *
     * @param int   $offset
     * @param mixed $value
     *
     * @throws  Exception
     *
     * @return  void
     */
    final public function offsetSet($offset, $value) : void
    {
        throw new Exception('Database results are read-only');
    }

    /**
     * Implements [ArrayAccess::offsetUnset], throws an error.
     *
     * [!!] You cannot modify a database result.
     *
     * @param int $offset
     *
     * @throws  Exception
     *
     * @return  void
     */
    final public function offsetUnset($offset) : void
    {
        throw new Exception('Database results are read-only');
    }

    /**
     * Implements [Iterator::key], returns the current row number.
     *
     * @return  integer
     */
    public function key() : int
    {
        return $this->_current_row;
    }

    /**
     * Implements [Iterator::next], moves to the next row.
     *
     * @return  self
     */
    public function next() : self
    {
        ++$this->_current_row;

        return $this;
    }

    /**
     * Implements [Iterator::prev], moves to the previous row.
     *
     * @return  self
     */
    public function prev() : self
    {
        --$this->_current_row;

        return $this;
    }

    /**
     * Implements [Iterator::rewind], sets the current row to zero.
     *
     * @return  self
     */
    public function rewind() : self
    {
        $this->_current_row = 0;

        return $this;
    }

    /**
     * Implements [Iterator::valid], checks if the current row exists.
     *
     * [!!] This method is only used internally.
     *
     * @return  boolean
     */
    public function valid() : bool
    {
        return $this->offsetExists($this->_current_row);
    }

}
