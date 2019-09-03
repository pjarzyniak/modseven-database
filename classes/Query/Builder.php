<?php
/**
 * Database query builder.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Query;

use Modseven\Database\Database;
use Modseven\Database\Exception;

abstract class Builder extends \Modseven\Database\Query {

    /**
     * Compiles an array of JOIN statements into an SQL partial.
     *
     * @param Database $db    Database instance
     * @param array    $joins Join statements
     *
     * @return  string
     */
    protected function _compile_join(Database $db, array $joins) : string
    {
        $statements = [];

        foreach ($joins as $join)
        {
            // Compile each of the join statements
            $statements[] = $join->compile($db);
        }

        return implode(' ', $statements);
    }

    /**
     * Compiles an array of conditions into an SQL partial. Used for WHERE
     * and HAVING.
     *
     * @param Database $db         Database instance
     * @param array    $conditions condition statements
     *
     * @return  string
     */
    protected function _compile_conditions(Database $db, array $conditions) : string
    {
        $last_condition = NULL;

        $sql = '';
        foreach ($conditions as $group)
        {
            // Process groups of conditions
            foreach ($group as $logic => $condition)
            {
                if ($condition === '(')
                {
                    if ( ! empty($sql) && $last_condition !== '(')
                    {
                        // Include logic operator
                        $sql .= ' '.$logic.' ';
                    }

                    $sql .= '(';
                }
                elseif ($condition === ')')
                {
                    $sql .= ')';
                }
                else
                {
                    if ( ! empty($sql) && $last_condition !== '(')
                    {
                        // Add the logic operator
                        $sql .= ' '.$logic.' ';
                    }

                    // Split the condition
                    [$column, $op, $value] = $condition;

                    if ($value === NULL)
                    {
                        if ($op === '=')
                        {
                            // Convert "val = NULL" to "val IS NULL"
                            $op = 'IS';
                        }
                        elseif ($op === '!=' || $op === '<>')
                        {
                            // Convert "val != NULL" to "valu IS NOT NULL"
                            $op = 'IS NOT';
                        }
                    }

                    // Database operators are always uppercase
                    $op = strtoupper($op);

                    if ($op === 'BETWEEN' && is_array($value))
                    {
                        // BETWEEN always has exactly two arguments
                        [$min, $max] = $value;

                        if ((is_string($min) && array_key_exists($min, $this->_parameters)) === FALSE)
                        {
                            // Quote the value, it is not a parameter
                            $min = $db->quote($min);
                        }

                        if ((is_string($max) && array_key_exists($max, $this->_parameters)) === FALSE)
                        {
                            // Quote the value, it is not a parameter
                            $max = $db->quote($max);
                        }

                        // Quote the min and max value
                        $value = $min.' AND '.$max;
                    }
                    elseif ($op === 'IN' && is_array($value) && count($value) === 0)
                    {
                        $value = '(NULL)';
                    }
                    elseif ((is_string($value) && array_key_exists($value, $this->_parameters)) === FALSE)
                    {
                        // Quote the value, it is not a parameter
                        $value = $db->quote($value);
                    }

                    if ($column)
                    {
                        if (is_array($column))
                        {
                            // Use the column name
                            $column = $db->quote_identifier(reset($column));
                        }
                        else
                        {
                            // Apply proper quoting to the column
                            $column = $db->quote_column($column);
                        }
                    }

                    // Append the statement to the query
                    $sql .= trim($column.' '.$op.' '.$value);
                }

                $last_condition = $condition;
            }
        }

        return $sql;
    }

    /**
     * Compiles an array of set values into an SQL partial. Used for UPDATE.
     *
     * @param Database $db     Database instance
     * @param array    $values updated values
     *
     * @return  string
     */
    protected function _compile_set(Database $db, array $values) : string
    {
        $set = [];
        foreach ($values as $group)
        {
            // Split the set
            [$column, $value] = $group;

            // Quote the column name
            $column = $db->quote_column($column);

            if ((is_string($value) && array_key_exists($value, $this->_parameters)) === FALSE)
            {
                // Quote the value, it is not a parameter
                $value = $db->quote($value);
            }

            $set[$column] = $column.' = '.$value;
        }

        return implode(', ', $set);
    }

    /**
     * Compiles an array of GROUP BY columns into an SQL partial.
     *
     * @param Database $db Database instance
     * @param array    $columns
     *
     * @return  string
     */
    protected function _compile_group_by(Database $db, array $columns) : string
    {
        $group = [];

        foreach ($columns as $column)
        {
            if (is_array($column))
            {
                // Use the column alias
                $column = $db->quote_identifier(end($column));
            }
            else
            {
                // Apply proper quoting to the column
                $column = $db->quote_column($column);
            }

            $group[] = $column;
        }

        return 'GROUP BY '.implode(', ', $group);
    }

    /**
     * Compiles an array of ORDER BY statements into an SQL partial.
     *
     * @param Database $db      Database instance
     * @param array    $columns sorting columns
     *
     * @throws Exception
     *
     * @return string
     */
    protected function _compile_order_by(Database $db, array $columns) : string
    {
        $sort = [];
        foreach ($columns as $group)
        {
            [$column, $direction] = $group;

            if (is_array($column))
            {
                // Use the column alias
                $column = $db->quote_identifier(end($column));
            }
            else
            {
                // Apply proper quoting to the column
                $column = $db->quote_column($column);
            }

            if ($direction)
            {
                // Make the direction uppercase
                $direction = ' '.strtoupper($direction);

                // Make sure direction is either ASC or DESC to prevent injections
                if ( ! in_array($direction, [
                    ' ASC',
                    ' DESC'
                ]))
                {
                    throw new Exception('Invalid sorting direction: '.$direction);
                }
            }

            $sort[] = $column.$direction;
        }

        return 'ORDER BY '.implode(', ', $sort);
    }

    /**
     * Reset the current builder status.
     *
     * @return  self
     */
    abstract public function reset();

}
