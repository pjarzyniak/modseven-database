<?php
/**
 * Database-based session class.
 *
 * Sample schema:
 *
 *     CREATE TABLE  `sessions` (
 *         `session_id` VARCHAR( 24 ) NOT NULL,
 *         `last_active` INT UNSIGNED NOT NULL,
 *         `contents` TEXT NOT NULL,
 *         PRIMARY KEY ( `session_id` ),
 *         INDEX ( `last_active` )
 *     ) ENGINE = MYISAM ;
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database;

use KO7\Cookie;

class Session extends \KO7\Session {

    /**
     * Database instance
     * @var Database
     */
    protected $_db;

    /**
     * Database table name
     * @var string
     */
    protected $_table = 'sessions';

    /**
     * Database column names
     * @var array|mixed
     */
    protected $_columns = [
        'session_id' => 'session_id',
        'last_active' => 'last_active',
        'contents' => 'contents'
    ];

    /**
     * Garbage collection requests
     * @var int
     */
    protected $_gc = 500;

    /**
     * The current session id
     * @var string
     */
    protected $_session_id;

    /**
     * The old session id
     * @var string
     */
    protected $_update_id;

    /**
     * Overloads the name, lifetime, and encrypted session settings.
     *
     * [!!] Sessions can only be created using the [Session::instance] method.
     *
     * @param array $config configuration
     * @param string $id session id
     *
     * @throws Exception
     * @throws \Exception
     * @throws \KO7\Exception
     */
    public function __construct(array $config = NULL, $id = NULL)
    {
        if ( ! isset($config['group']))
        {
            // Use the default group
            $config['group'] = Database::$default;
        }

        // Load the database
        $this->_db = Database::instance($config['group']);

        if (isset($config['table']))
        {
            // Set the table name
            $this->_table = (string)$config['table'];
        }

        if (isset($config['gc']))
        {
            // Set the gc chance
            $this->_gc = (int)$config['gc'];
        }

        if (isset($config['columns']))
        {
            // Overload column names
            $this->_columns = $config['columns'];
        }

        parent::__construct($config, $id);

        if (random_int(0, $this->_gc) === $this->_gc)
        {
            // Run garbage collection
            // This will average out to run once every X requests
            $this->_gc();
        }
    }

    /**
     * Get the current session id, if the session supports it.
     *
     * @return  string
     */
    public function id() : string
    {
        return $this->_session_id;
    }

    /**
     * Loads the raw session data string and returns it.
     *
     * @param string $id session id
     *
     * @throws \KO7\Exception
     *
     * @return  string
     */
    protected function _read(?string $id = NULL) : string
    {
        if ($id || $id = Cookie::get($this->_name))
        {
            $result = DB::select([
                $this->_columns['contents'],
                'contents'
            ])->from($this->_table)->where($this->_columns['session_id'], '=', ':id')->limit(1)->param(':id', $id)->execute($this->_db);

            if ($result->count())
            {
                // Set the current session id
                $this->_session_id = $this->_update_id = $id;

                // Return the contents
                return $result->get('contents');
            }
        }

        // Create a new session id
        $this->_regenerate();

        return NULL;
    }

    /**
     * Generate a new session id and return it.
     *
     * @throws \KO7\Exception
     *
     * @return  string
     */
    protected function _regenerate() : string
    {
        // Create the query to find an ID
        $query = DB::select($this->_columns['session_id'])
            ->from($this->_table)
            ->where($this->_columns['session_id'], '=', ':id')
            ->limit(1)->bind(':id', $id
        );

        do
        {
            // Create a new session id
            $id = str_replace('.', '-', uniqid(NULL, TRUE));

            // Get the the id from the database
            $result = $query->execute($this->_db);
        }
        while ($result->count());

        return $this->_session_id = $id;
    }

    /**
     * Writes the current session.
     *
     * @throws \KO7\Exception
     *
     * @return  boolean
     */
    protected function _write() : bool
    {
        if ($this->_update_id === NULL)
        {
            // Insert a new row
            $query = DB::insert($this->_table, $this->_columns)->values([
                    ':new_id',
                    ':active',
                    ':contents'
                ]);
        }
        else
        {
            // Update the row
            $query = DB::update($this->_table)
                ->value($this->_columns['last_active'], ':active')
                ->value($this->_columns['contents'], ':contents')
                ->where($this->_columns['session_id'], '=', ':old_id');

            if ($this->_update_id !== $this->_session_id)
            {
                // Also update the session id
                $query->value($this->_columns['session_id'], ':new_id');
            }
        }

        $query->param(':new_id', $this->_session_id)
            ->param(':old_id', $this->_update_id)
            ->param(':active', $this->_data['last_active'])
            ->param(':contents', (string)$this);

        // Execute the query
        $query->execute($this->_db);

        // The update and the session id are now the same
        $this->_update_id = $this->_session_id;

        // Update the cookie with the new session id
        Cookie::set($this->_name, $this->_session_id, $this->_lifetime);

        return TRUE;
    }

    /**
     * Restart the session.
     *
     * @throws \KO7\Exception
     *
     * @return  boolean
     */
    protected function _restart() : bool
    {
        $this->_regenerate();

        return TRUE;
    }

    /**
     * Completely destroy the current session.
     *
     * @return  boolean
     */
    protected function _destroy() : bool
    {
        if ($this->_update_id === NULL)
        {
            // Session has not been created yet
            return TRUE;
        }

        // Delete the current session
        $query = DB::delete($this->_table)
            ->where($this->_columns['session_id'], '=', ':id')
            ->param(':id', $this->_update_id);

        try
        {
            // Execute the query
            $query->execute($this->_db);

            // Delete the old session id
            $this->_update_id = NULL;

            // Delete the cookie
            Cookie::delete($this->_name);
        }
        catch (\Exception $e)
        {
            // An error occurred, the session has not been deleted
            return FALSE;
        }

        return TRUE;
    }

    /**
     * Garbage Collector
     *
     * @throws \KO7\Exception
     */
    protected function _gc() : void
    {
        if ($this->_lifetime)
        {
            // Expire sessions when their lifetime is up
            $expires = $this->_lifetime;
        }
        else
        {
            // Expire sessions after one month
            $expires = \KO7\Date::MONTH;
        }

        // Delete all sessions that have expired
        DB::delete($this->_table)
            ->where($this->_columns['last_active'], '<', ':time')
            ->param(':time', time()-$expires)->execute($this->_db);
    }

}
