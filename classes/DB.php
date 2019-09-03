<?php
/**
 * Provides a shortcut to get Database related objects for making queries.
 *
 * You pass the same parameters to these functions as you pass to the objects they return.
 *
 * @copyright  (c) 2007-2016  Kohana Team
 * @copyright  (c) since 2016 Koseven Team
 * @license        https://koseven.ga/LICENSE
 */

namespace Modseven\Database;

use Modseven\Database\Expression;
use Modseven\Database\Query\Builder\Select;
use Modseven\Database\Query\Builder\Insert;
use Modseven\Database\Query\Builder\Update;
use Modseven\Database\Query\Builder\Delete;

class DB {

    /**
     * Create a new Database_Query of the given type.
     *
     * Specifying the type changes the returned result. When using
     * `Database::SELECT`, a [Database_Query_Result] will be returned.
     * `Database::INSERT` queries will return the insert id and number of rows.
     * For all other queries, the number of affected rows is returned.
     *
     * @param integer $type type: Database::SELECT, Database::UPDATE, etc
     * @param string  $sql  SQL statement
     *
     * @return  Query
     */
    public static function query(int $type, string $sql) : Query
    {
        return new Query($type, $sql);
    }

    /**
     * Create a new Database_Query_Builder_Select. Each argument will be
     * treated as a column. To generate a `foo AS bar` alias, use an array.
     *
     * @param mixed $columns column name or array($column, $alias) or object
     *
     * @return  Select
     */
    public static function select($columns = NULL) : Select
    {
        return new Select(func_get_args());
    }

    /**
     * Create a new Database_Query_Builder_Select from an array of columns.
     *
     * @param array $columns columns to select
     *
     * @return  Select
     */
    public static function select_array(array $columns = NULL) : Select
    {
        return new Select($columns);
    }

    /**
     * Create a new Database_Query_Builder_Insert.
     *
     * @param string $table   table to insert into
     * @param array  $columns list of column names or array($column, $alias) or object
     *
     * @return  Insert
     */
    public static function insert(string $table = NULL, array $columns = NULL) : Insert
    {
        return new Insert($table, $columns);
    }

    /**
     * Create a new Database_Query_Builder_Update.
     *
     * @param string $table table to update
     *
     * @return  Update
     */
    public static function update(string $table = NULL) : Update
    {
        return new Update($table);
    }

    /**
     * Create a new Database_Query_Builder_Delete.
     *
     * @param string $table table to delete from
     *
     * @return  Delete
     */
    public static function delete(string $table = NULL) : Delete
    {
        return new Delete($table);
    }

    /**
     * Create a new [Database_Expression] which is not escaped. An expression
     * is the only way to use SQL functions within query builders.
     *
     * @param string $exp         Expression
     * @param array  $parameters  Params [optional]
     *
     * @return  Expression
     */
    public static function expr(string $exp, $parameters = []) : Expression
    {
        return new Expression($exp, $parameters);
    }

}