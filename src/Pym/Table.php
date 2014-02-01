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

    protected $selects = array();
    protected $wheres = array();
    protected $leftJoin = '';
    protected $groupBy = '';
    protected $orderBy = '';
    protected $limit = '';

    public function __construct(Connection $db = null, $tableName, $tableAlias = null, $tablesAliases = null)
    {
        $this->db = $db;
        $this->tableName = $tableName;
        $this->tableAlias = $tableAlias;
        $this->tablesAliases = $tablesAliases;
        $this->table = $this->tableAlias !== null ? $this->tableAlias : "`$this->tableName`";
        if ($db !== null) {
            $tableColumns = $db->executeQuery("DESCRIBE $tableName")->fetchAll(\PDO::FETCH_COLUMN);
            $this->isTimestampable = count(array_intersect(['created_at', 'updated_at'], $tableColumns)) == 2;
            $this->isSoftdeletable = in_array('deleted_at', $tableColumns);
        }
    }

    public function getTableName()
    {
        return $this->tableName;
    }

    public function getTableNameOrAlias()
    {
        return $this->tableAlias !== null ? $this->tableAlias : "`$this->tableName`";
    }

    public function getQuery()
    {
        if ($this->isSoftdeletable) {
            $this->where([$this->table.'.deleted_at' => null]);
        }

        return $this->buildSQL();
    }

    protected function escapeColumns(array $columns) {
        array_walk($columns, function(&$value) {
            $escapaUnlessIsTableAlias = function($value) {
                return in_array($value, $this->tablesAliases) ? $value : "`$value`";
            };
            if (preg_match('/^(\w+)\((\*|\w+)\.?(\w+)?\)$/i', $value, $matches)) {
                if ($matches[2] === '*') {
                    $value = sprintf('%s(*) AS %s', $matches[1], strtolower($this->tableAlias.'_'.$matches[1]));
                } else {
                    $table = $escapaUnlessIsTableAlias($matches[2]);
                    $column = isset($matches[3]) ? "$table.`$matches[3]`" : $table;
                    $value = sprintf('%s(%s) AS %s', $matches[1], $column, strtolower($matches[2].'_'.$matches[1]));
                }
            } elseif (preg_match('/^(\w+)\.(\w+|\*{1})(?:\s+(?:[aA][sS]\s+)?)?(.+)?$/i', $value, $matches)) {
                $alias = isset($matches[3]) ? ' AS ' . $matches[3] : '';
                $value = sprintf('%s.%s%s', $escapaUnlessIsTableAlias($matches[1]), $matches[2] === '*' ? '*' : "`$matches[2]`", $alias);
            } elseif (!is_int($value) && !preg_match('/^\w+\s\*$/i', $value)) {
                $value = "`$value`";
            }
        });

        return $columns;
    }

    protected function cleanData(array $data) {
        $keys = array_keys($data);
        array_walk($keys, function(&$value) {
            $value = "`$value`";
        });

        return array_combine($keys, array_values($data));
    }

    protected function buildSQL()
    {
        $sql = $this->buildSelect() . $this->leftJoin . $this->buildWhere() . $this->groupBy . $this->orderBy . $this->limit;

        $this->leftJoin = $this->groupBy = $this->orderBy = $this->limit = '';
        $this->selects = $this->wheres = [];

        return $sql;
    }

    public function select($columns)
    {
        if (is_array($columns)) {
            foreach ($columns as $column) {
                $this->selects[] = $column;
            }
        } else {
            $this->selects[] = $columns;
        }

        return $this;
    }

    protected function buildSelect()
    {
        return sprintf(
            'SELECT %s FROM `%s` %s',
                count($this->selects) ? implode(', ', $this->escapeColumns(array_unique($this->selects))) : '*',
                $this->tableName,
                $this->tableAlias
        );
    }

    public function where(array $where, $operator = 'AND')
    {
        $collection = [];
        foreach ($where as $key => $value) {
            $collection[$key] = $value;
        }

        $this->wheres[] = [
            'operator'   => $operator,
            'collection' => $collection
        ];

        return $this;
    }

    public function orWhere(array $where)
    {
        return $this->where($where, 'OR');
    }

    protected function buildWhere()
    {
        if (count($this->wheres)) {
            $wheres = array_combine($this->escapeColumns(array_keys($this->wheres)), $this->wheres);

            $whereList = [];
            foreach ($wheres as $where) {
                $currentWhereCollection = [];

                foreach ($where['collection'] as $key => $value) {
                    $currentWhereCollection[] = sprintf('%s %s ?', $key, $value === null ? 'IS' : '=');
                }

                $currentWhereString = implode(sprintf(' %s ', $where['operator']), $currentWhereCollection);
                if ($where['operator'] === 'OR') $currentWhereString = "($currentWhereString)";

                $whereList[] = $currentWhereString;
                $this->queryParams = array_merge($this->queryParams, array_values($where['collection']));
            }

            return ' WHERE ' . implode(' AND ', $whereList);
        }

        return '';
    }

    public function leftJoin($rightTable, $onLeft, $onRight)
    {
        $rightTable = explode(' ', $rightTable);
        $rightTableName = "`$rightTable[0]`";
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
        $keys = $values = [];
        foreach ($columns as $key => $value) {
            if (is_int($key)) {
                $keys[] = $value;
                $values[] = '';
            } else {
                $keys[] = $key;
                $values[] = $value;
            }
        }

        $columns = array_combine($this->escapeColumns($keys), $values);
        array_walk($columns, function (&$value, $key) {
            $value = $key . (empty($value) ? '' : ' '.$value);
        });

        $this->orderBy = sprintf(' ORDER BY %s', implode(', ', $columns));

        return $this;
    }

    public function limit($start, $end = null)
    {
        $this->limit = sprintf(' LIMIT %s%s', $start, $end === null ?: ", $end");

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

    public function insert(array $data, $returnAffectedRowsCount = false)
    {
        if ($this->isTimestampable) {
            $now = get_object_vars(new \DateTime('now'))['date'];
            $data['created_at'] = $now;
            $data['updated_at'] = $now;
        }

        $affectedRowsCount = $this->db->insert($this->tableName, $this->cleanData($data));

        return $returnAffectedRowsCount ? $affectedRowsCount : $this->db->lastInsertId();
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
