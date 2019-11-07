<?php
/**
 * Database query builder for WHERE statements.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license    https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Query\Builder;

use Modseven\Database\Query\Builder;

abstract class Where extends Builder
{
    /**
     * Where clause
     * @var array
     */
    protected $_where = [];

    /**
     * Order by clause
     * @var array
     */
    protected $_order_by = [];

    /**
     * Limit?
     * @var int
     */
    protected $_limit;

    /**
     * Alias of and_where()
     *
     * @param mixed  $column column name or array($column, $alias) or object
     * @param string $op     logic operator
     * @param mixed  $value  column value
     *
     * @return  self
     */
    public function where($column, string $op, $value) : self
    {
        return $this->andWhere($column, $op, $value);
    }

    /**
     * Creates a new "AND WHERE" condition for the query.
     *
     * @param mixed  $column column name or array($column, $alias) or object
     * @param string $op     logic operator
     * @param mixed  $value  column value
     *
     * @return  self
     */
    public function andWhere($column, string $op, $value) : self
    {
        $this->_where[] = [
            'AND' => [
                $column,
                $op,
                $value
            ]
        ];

        return $this;
    }

    /**
     * Creates a new "OR WHERE" condition for the query.
     *
     * @param mixed  $column column name or array($column, $alias) or object
     * @param string $op     logic operator
     * @param mixed  $value  column value
     *
     * @return  self
     */
    public function orWhere($column, string $op, $value) : self
    {
        $this->_where[] = [
            'OR' => [
                $column,
                $op,
                $value
            ]
        ];

        return $this;
    }

    /**
     * Alias of and_where_open()
     *
     * @return  self
     */
    public function whereOpen() : self
    {
        return $this->andWhereOpen();
    }

    /**
     * Opens a new "AND WHERE (...)" grouping.
     *
     * @return  self
     */
    public function andWhereOpen() : self
    {
        $this->_where[] = ['AND' => '('];

        return $this;
    }

    /**
     * Opens a new "OR WHERE (...)" grouping.
     *
     * @return  self
     */
    public function orWhereOpen() : self
    {
        $this->_where[] = ['OR' => '('];

        return $this;
    }

    /**
     * Closes an open "WHERE (...)" grouping.
     *
     * @return  self
     */
    public function whereClose() : self
    {
        return $this->andWhereClose();
    }

    /**
     * Closes an open "WHERE (...)" grouping or removes the grouping when it is
     * empty.
     *
     * @return  self
     */
    public function whereCloseEmpty() : self
    {
        $group = end($this->_where);

        if ($group && reset($group) === '(')
        {
            array_pop($this->_where);

            return $this;
        }

        return $this->whereClose();
    }

    /**
     * Closes an open "WHERE (...)" grouping.
     *
     * @return  self
     */
    public function andWhereClose() : self
    {
        $this->_where[] = ['AND' => ')'];

        return $this;
    }

    /**
     * Closes an open "WHERE (...)" grouping.
     *
     * @return  self
     */
    public function orWhereClose() : self
    {
        $this->_where[] = ['OR' => ')'];

        return $this;
    }

    /**
     * Applies sorting with "ORDER BY ..."
     *
     * @param mixed  $column    column name or array($column, $alias) or object
     * @param string $direction direction of sorting
     *
     * @return  self
     */
    public function orderBy($column, ?string $direction = NULL) : self
    {
        $this->_order_by[] = [
            $column,
            $direction
        ];

        return $this;
    }

    /**
     * Return up to "LIMIT ..." results
     *
     * @param integer $number maximum results to return or NULL to reset
     *
     * @return  self
     */
    public function limit(int $number) : self
    {
        $this->_limit = $number;

        return $this;
    }

}
