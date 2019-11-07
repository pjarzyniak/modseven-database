<?php
/**
 * Object used for caching the results of select queries.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license    https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Result;

use Modseven\Database\Result;

class Cached extends Result
{
    /**
     * Sets the total number of rows and stores the result locally.
     *
     * @param mixed  $result        Query result
     * @param string $sql           SQL query
     * @param mixed  $as_object     As Object
     * @param array  $object_params Object Parameter
     */
    public function __construct(array $result, string $sql, $as_object = NULL, ?array $object_params = NULL)
    {
        parent::__construct($result, $sql, $as_object, $object_params);

        // Find the number of rows in the result
        $this->_total_rows = count($result);
    }

    /**
     * Returns itself
     * @return self
     */
    public function cached() : self
    {
        return $this;
    }

    /**
     * Seeks to given offset
     *
     * @param int $offset Offset to seek to
     *
     * @return bool
     */
    public function seek($offset) : bool
    {
        if ($this->offsetExists($offset))
        {
            $this->_current_row = $offset;

            return TRUE;
        }

        return FALSE;
    }

    /**
     * Get current row
     *
     * @return mixed
     */
    public function current()
    {
        // Return an array of the row
        return $this->valid() ? $this->_result[$this->_current_row] : NULL;
    }

    /**
     * Result destruction cleans up all open result sets.
     */
    public function __destruct() {}

}
