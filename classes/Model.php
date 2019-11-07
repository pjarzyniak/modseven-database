<?php
/**
 * Database Model base class.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license    https://koseven.ga/LICENSE
 */

namespace Modseven\Database;

abstract class Model extends \Modseven\Model
{
    /**
     * Database instance
     * @var Database
     */
    protected $_db;

    /**
     * Create a new model instance. A [Database] instance or configuration
     * group name can be passed to the model. If no database is defined, the
     * "default" database group will be used.
     *
     * @param   string   $name  model name
     * @param   mixed    $db    Database instance object or string
     *
     * @return  \Modseven\Model
     */
    public static function factory(string $name, $db = NULL) : \Modseven\Model
    {
        // Add the model prefix
        $class = 'Model_'.$name;

        return new $class($db);
    }

    /**
     * Loads the database.
     *
     * @param mixed $db Database instance object or string
     *
     * @throws Exception
     * @throws \Modseven\Exception
     *
     * @return  void
     */
    public function __construct($db = NULL)
    {
        if ($db)
        {
            // Set the instance or name
            $this->_db = $db;
        }
        elseif ( ! $this->_db)
        {
            // Use the default name
            $this->_db = Database::$default;
        }

        if (is_string($this->_db))
        {
            // Load the database
            $this->_db = Database::instance($this->_db);
        }
    }

}
