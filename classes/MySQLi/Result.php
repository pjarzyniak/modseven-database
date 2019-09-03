<?php
/**
 * MySQLi database result.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database\MySQLi;

class Result extends \Modseven\Database\Result {

    protected $_internal_row = 0;

    /**
     * Sets the total number of rows and stores the result locally.
     *
     * @param   mixed   $result     query result
     * @param   string  $sql        SQL query
     * @param   mixed   $as_object
     * @param   array   $params
     * @return  void
     */
    public function __construct($result, string $sql, $as_object = FALSE, array $params = NULL)
    {
        // Call parent
        parent::__construct($result, $sql, $as_object, $params);

        // Find the number of rows in the result
        $this->_total_rows = $result->num_rows;
    }

    /**
     * Class destructor
     * Free up resources
     */
    public function __destruct()
    {
        if (is_resource($this->_result))
        {
            $this->_result->free();
        }
    }

    /**
     * Seeks to a position
     *
     * @param int $offset The position to seek to.
     *
     * @return bool
     */
    public function seek($offset) : bool
    {
        if ($this->offsetExists($offset) && $this->_result->data_seek($offset))
        {
            // Set the current row to the offset
            $this->_current_row = $this->_internal_row = $offset;

            return TRUE;
        }

        return FALSE;
    }

    /**
     * Fetch current Row
     *
     * @return mixed
     */
    public function current()
    {
        if ($this->_current_row !== $this->_internal_row && ! $this->seek($this->_current_row))
        {
            return NULL;
        }

        // Increment internal row for optimization assuming rows are fetched in order
        $this->_internal_row++;

        if ($this->_as_object === TRUE)
        {
            // Return an stdClass
            return $this->_result->fetch_object();
        }
        if (is_string($this->_as_object))
        {
            // Return an object of given class name
            return $this->_result->fetch_object($this->_as_object, (array)$this->_object_params);
        }

        // Return an array of the row
        return $this->_result->fetch_assoc();
    }

}
