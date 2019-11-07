<?php
/**
 * Database reader for the Modseven config system
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license    https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Config;

use Modseven\Database\DB;
use Modseven\Database\Database;
use Modseven\Database\Exception;

class Reader implements \Modseven\Config\Reader
{
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
            $this->_db_instance = Database::$default;
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
     *
     * @throws \Modseven\Exception
     */
    public function load(string $group)
    {
        /**
         * Prevents the catch-22 scenario where the database config reader attempts to load the
         * database connections details from the database.
         */
        if ($group === 'database')
        {
            return FALSE;
        }

        try
        {
            $query = DB::select('config_key', 'config_value')
                       ->from($this->_table_name)->where('group_name', '=', $group)
                       ->execute($this->_db_instance);
        }
        catch (Exception $e)
        {
            throw new \Modseven\Exception($e->getMessage(), null, $e->getCode(), $e);
        }


        return count($query) ? array_map('unserialize', $query->as_array('config_key', 'config_value')) : FALSE;
    }

}
