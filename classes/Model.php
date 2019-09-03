<?php
/**
 * Database Model base class.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database;

abstract class Model extends \KO7\Model {

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
     * @return  \KO7\Model
     */
    public static function factory(string $name, $db = NULL) : \KO7\Model
    {
        // Add the model prefix
        $class = 'Model_'.$name;

        return new $class($db);
    }

    /**
     * Loads the database.
     *
     *     $model = new Foo_Model($db);
     *
     * @param mixed $db Database instance object or string
     *
     * @throws Exception
     * @throws \KO7\Exception
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
