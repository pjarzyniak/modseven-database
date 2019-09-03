<?php
/**
 * Database reader for the KO7 config system
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Config;

class Reader implements \KO7\Config\Reader {

    /**
     * Database instance name
     * @var string
     */
    protected $_db_instance;

    /**
     * Configuration Table Name
     * @var mixed|string
     */
    protected $_table_name = 'config';

    /**
     * Constructs the database reader object
     *
     * @param array Configuration for the reader
     */
    public function __construct(array $config = NULL)
    {
        if (isset($config['instance']))
        {
            $this->_db_instance = $config['instance'];
        }
        elseif ($this->_db_instance === NULL)
        {
            $this->_db_instance = \Modseven\Database\Database::$default;
        }

        if (isset($config['table_name']))
        {
            $this->_table_name = $config['table_name'];
        }
    }

    /**
     * Tries to load the specified configuration group
     *
     * Returns FALSE if group does not exist or an array if it does
     *
     * @param string $group Configuration group
     *
     * @return boolean|array
     */
    public function load(string $group)
    {
        /**
         * Prevents the catch-22 scenario where the database config reader attempts to load the
         * database connections details from the database.
         *
         * @link http://github.com/koseven/koseven/issues/4316
         */
        if ($group === 'database')
        {
            return FALSE;
        }

        $query = \Modseven\Database\DB::select('config_key', 'config_value')
            ->from($this->_table_name)->where('group_name', '=', $group)
            ->execute($this->_db_instance);

        return count($query) ? array_map('unserialize', $query->as_array('config_key', 'config_value')) : FALSE;
    }

}
