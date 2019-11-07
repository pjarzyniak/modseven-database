<?php
/**
 * Database writer for the config system
 *
 * Schema for configuration table:
 *
 *    CREATE TABLE IF NOT EXISTS `config` (
 *      `group_name` varchar(128) NOT NULL,
 *      `config_key` varchar(128) NOT NULL,
 *      `config_value` text,
 *       PRIMARY KEY (`group_name`,`config_key`)
 *     ) ENGINE=InnoDB;
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) 2016-2019  Koseven Team
 * @copyright  (c) since 2019 Modseven Team
 * @license    https://koseven.ga/LICENSE
 */

namespace Modseven\Database\Config;

use Modseven\Database\DB;
use Modseven\Exception;

class Writer extends Reader implements \Modseven\Config\Writer
{
    /**
     * Holds already loaded keys
     * @var array
     */
    protected $_loaded_keys = [];

    /**
     * Tries to load the specified configuration group
     *
     * Returns FALSE if group does not exist or an array if it does
     *
     * @param string $group Configuration group
     *
     * @return boolean|array
     *
     * @throws Exception;
     */
    public function load(string $group)
    {
        $config = parent::load($group);

        if ($config !== FALSE)
        {
            $this->_loaded_keys[$group] = array_combine(array_keys($config), array_keys($config));
        }

        return $config;
    }

    /**
     * Writes the passed config for $group
     *
     * Returns chainable instance on success or throws
     * Exception on failure
     *
     * @param string $group  The config group
     * @param string $key    The config key to write to
     * @param array  $config The configuration to write
     *
     * @return boolean
     */
    public function write(string $group, string $key, array $config) : bool
    {
        $conf = serialize($config);

        // Check to see if we've loaded the config from the table already
        if (isset($this->_loaded_keys[$group][$key]))
        {
            $this->_update($group, $key, $conf);
        }
        else
        {
            // Attempt to run an insert query
            // This may fail if the config key already exists in the table
            // and we don't know about it
            try
            {
                $this->_insert($group, $key, $conf);
            }
            catch (\Modseven\Database\Exception $e)
            {
                // Attempt to run an update instead
                $this->_update($group, $key, $conf);
            }
        }

        return TRUE;
    }

    /**
     * Insert the config values into the table
     *
     * @param string $group  The config group
     * @param string $key    The config key to write to
     * @param string $config The serialized configuration to write
     *
     * @return self
     *
     * @throws Exception
     */
    protected function _insert(string $group, string $key, string $config) : self
    {
        DB::insert($this->_table_name, [
            'group_name',
            'config_key',
            'config_value'
        ])->values([
                $group,
                $key,
                $config
            ])->execute($this->_db_instance);

        return $this;
    }

    /**
     * Update the config values in the table
     *
     * @param string $group  The config group
     * @param string $key    The config key to write to
     * @param string $config The serialized configuration to write
     *
     * @return self
     *
     * @throws Exception
     */
    protected function _update(string $group, string $key, string $config) : self
    {
        DB::update($this->_table_name)
            ->set(['config_value' => $config])
            ->where('group_name', '=', $group)
            ->where('config_key', '=', $key)
            ->execute($this->_db_instance);

        return $this;
    }

}
