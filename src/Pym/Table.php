<?php
namespace Pym;

use Doctrine\DBAL\Connection;

class Table
{
    protected $db = null;
    protected $tableName;
    protected $tableAlias;
    protected $table;
    protected $isTimestampable = false;
    protected $isSoftdeletable = false;
    protected $queryParams = array();

    protected $select = '';
    protected $wheres = array();
    protected $leftJoin = '';
    protected $groupBy = '';
    protected $orderBy = '';

    public function __construct(Connection $db, $tableName, $tableAlias = null, $tablesAliases = null)
    {
        $this->db = $db;
        $this->tableName = $tableName;
        $this->tableAlias = $tableAlias;
        $this->tablesAliases = $tablesAliases;
        $this->table = $this->tableAlias !== null ? $this->tableAlias : '`'.$this->tableName.'`';
        $tableColumns = $db->executeQuery("DESCRIBE $tableName")->fetchAll(\PDO::FETCH_COLUMN);
        $this->isTimestampable = count(array_intersect(['created_at', 'updated_at'], $tableColumns)) == 2;
        $this->isSoftdeletable = in_array('deleted_at', $tableColumns);
    }

    public function getTableName()
    {
        return $this->tableName;
    }

    public function getTableNameOrAlias()
    {
        return $this->tableAlias !== null ? $this->tableAlias : '`'.$this->tableName.'`';
    }

    public function getQuery()
    {
        if (empty($this->select)) $this->select();

        if ($this->isSoftdeletable) {
            $this->where([$this->table.'.deleted_at' => null]);
        }

        return $this->buildSQL();
    }

    protected function escapeColumns(array $columns) {
        array_walk($columns, function(&$value) {
            if (preg_match('/^(\w+)\((\*|\w+)\.?(\w+)?\)$/i', $value, $matches)) {
                if ($matches[2] != '*') {
                    $_matches[] = [];
                    for ($i=1; $i <= 3; $i++) {
                        $_matches[$i] = '`'.$matches[$i].'`';
                    }
                    $column = isset($matches[3]) ? implode('.', array_slice($_matches, 2)) : $_matches[2];
                    $value = sprintf('%s(%s) AS %s', $matches[1], $column, strtolower($matches[2].'_'.$matches[1]));
                }
            } elseif (preg_match('/^(\w+)\.(\w+|\*{1})(?:\sAS\s)?(.+)?$/i', $value, $matches)) {
                $table = in_array($matches[1], $this->tablesAliases) ? $matches[1] : '`'.$matches[1].'`';
                $alias = isset($matches[3]) ? ' AS ' . $matches[3] : ' ';
                $value = sprintf('%s.%s%s', $table, $matches[2] === '*' ? '*' : "`$matches[2]`", $alias);
            } else {
                $value = '`'.$value.'`';
            }
        });

        return $columns;
    }

    protected function cleanData(array $data) {
        $keys = array_keys($data);
        array_walk($keys, function(&$value) {
            $value = '`'.$value.'`';
        });

        return array_combine($keys, array_values($data));
    }

    protected function buildSQL()
    {
        $sql = $this->select . $this->leftJoin . $this->buildWhere() . $this->groupBy . $this->orderBy;

        $this->select = $this->leftJoin = $this->groupBy = $this->orderBy = '';
        $this->wheres = [];

        return $sql;
    }

    public function select($columns = null)
    {
        if ($columns === null) {
            $select = '*';
        } else {
            $select = implode(', ', $this->escapeColumns(is_array($columns) ? $columns : [$columns]));
        }

        $this->select = sprintf('SELECT %s FROM `%s` %s', $select, $this->tableName, $this->tableAlias);

        return $this;
    }

    public function where(array $where)
    {
        foreach ($where as $key => $value) {
            $this->wheres[$key] = $value;
        }

        return $this;
    }

    protected function buildWhere()
    {
        if (count($this->wheres)) {
            $where = array_combine($this->escapeColumns(array_keys($this->wheres)), $this->wheres);

            $whereArray = [];
            foreach ($where as $key => $value) {
                $whereArray[] = sprintf('%s %s ?', $key, $value === null ? 'IS' : '=');
            }

            $this->queryParams = array_values($where);

            return ' WHERE ' . implode(' AND ', $whereArray);
        }

        return '';
    }

    public function leftJoin($rightTable, $onLeft, $onRight)
    {
        $rightTable = explode(' ', $rightTable);
        $rightTableName = '`'.$rightTable[0].'`';
        $rightTableAlias = isset($rightTable[1]) ? $rightTable[1] : '';

        $this->leftJoin .= sprintf(
            ' LEFT JOIN %s %s ON %s = %s',
            $rightTableName,
            $rightTableAlias,
            $onLeft,
            $onRight
        );

        return $this;
    }

    public function groupBy(array $columns)
    {
        $this->groupBy = sprintf(' GROUP BY %s', implode(', ', $this->escapeColumns($columns)));

        return $this;
    }

    public function orderBy(array $columns)
    {
        $this->orderBy = sprintf(' ORDER BY %s', implode(', ', $this->escapeColumns($columns)));

        return $this;
    }

    public function count()
    {
        $this->select('COUNT(*)');

        return $this;
    }

    public function fetchAll(array $where = array())
    {
        if (count($where)) $this->where($where);

        return $this->db->fetchAll($this->getQuery(), $this->queryParams);
    }

    public function fetchAllKeyPair()
    {
        return $this->db->executeQuery($this->getQuery(), $this->queryParams)->fetchAll(\PDO::FETCH_KEY_PAIR);
    }

    public function fetchAllKeyPairs()
    {
        $results = $this->db->executeQuery($this->getQuery(), $this->queryParams)->fetchAll(\PDO::FETCH_GROUP|\PDO::FETCH_ASSOC);

        return array_map('reset', $results);
    }

    public function fetchAssoc(array $where = array())
    {
        if (count($where)) $this->where($where);

        return $this->db->fetchAssoc($this->getQuery(), $this->queryParams);
    }

    public function executeQuery($query, array $queryParams = array())
    {
        if (count($queryParams) === 0) {
            $queryParams = $this->queryParams;
        }

        return $this->db->executeQuery($query, $queryParams);
    }

    public function insert(array $data)
    {
        if ($this->isTimestampable) {
            $now = get_object_vars(new \DateTime('now'))['date'];
            $data['created_at'] = $now;
            $data['updated_at'] = $now;
        }

        return $this->db->insert($this->tableName, $this->cleanData($data));
    }

    public function update(array $data, array $where)
    {
        if ($this->isTimestampable) {
            $data['updated_at'] = get_object_vars(new \DateTime('now'))['date'];
        }

        return $this->db->update($this->tableName, $this->cleanData($data), $where);
    }

    public function delete(array $where)
    {
        if ($this->isSoftdeletable) {
            return $this->db->update($this->tableName, ['deleted_at' => get_object_vars(new \DateTime('now'))['date']], $where);
        }

        return $this->db->delete($this->tableName, $where);
    }

    public function drop()
    {
        return $this->db->query(sprintf('DELETE FROM `%s`', $this->tableName));
    }

}
