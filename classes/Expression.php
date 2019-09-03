<?php
/**
 * Database expressions can be used to add unescaped SQL fragments to a
 * [Database_Query_Builder] object.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database;

class Expression {

    /**
     * Unquoted parameters
     * @var array
     */
    protected $_parameters;

    /**
     * Raw expression string
     * @var string
     */
    protected $_value;

    /**
     * Sets the expression string.
     *
     * @param string $value raw SQL expression string
     * @param array  $parameters unquoted parameter values
     */
    public function __construct(string $value, array $parameters = [])
    {
        // Set the expression string
        $this->_value = $value;
        $this->_parameters = $parameters;
    }

    /**
     * Bind a variable to a parameter.
     *
     * @param string $param parameter key to replace
     * @param mixed  $var   variable to use
     *
     * @return  self
     */
    public function bind(string $param, &$var) : self
    {
        $this->_parameters[$param] =& $var;

        return $this;
    }

    /**
     * Set the value of a parameter.
     *
     * @param string $param parameter key to replace
     * @param mixed  $value value to use
     *
     * @return  self
     */
    public function param(string $param, $value) : self
    {
        $this->_parameters[$param] = $value;

        return $this;
    }

    /**
     * Add multiple parameter values.
     *
     * @param array $params list of parameter values
     *
     * @return  self
     */
    public function parameters(array $params) : self
    {
        $this->_parameters = $params+$this->_parameters;

        return $this;
    }

    /**
     * Get the expression value as a string.
     *
     * @return  string
     */
    public function value() : string
    {
        return (string)$this->_value;
    }

    /**
     * Return the value of the expression as a string.
     *
     * @return  string
     */
    public function __toString() : string
    {
        return $this->value();
    }

    /**
     * Compile the SQL expression and return it. Replaces any parameters with
     * their given values.
     *
     * @param mixed    Database instance or name of instance
     *
     * @throws Exception;
     * @throws \KO7\Exception;
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

        $value = $this->value();

        if ( ! empty($this->_parameters))
        {
            // Quote all of the parameter values
            $params = array_map([
                $db,
                'quote'
            ], $this->_parameters);

            // Replace the values in the expression
            $value = strtr($value, $params);
        }

        return $value;
    }

}
