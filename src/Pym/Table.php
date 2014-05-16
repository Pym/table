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
        if ($db !== null && $db->getParams()['driver'] !== 'pdo_sqlite') {
            $tableColumns = $db->executeQuery("DESCRIBE $tableName")->fetchAll(\PDO::FETCH_COLUMN);
            $this->isTimestampable = count(array_intersect(['created_at', 'updated_at'], $tableColumns)) == 2;
            $this->isSoftdeletable = in_array('deleted_at', $tableColumns);
        }
    }

    public function getConnection()
    {
        return $this->db;
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

    public function getQueryParams()
    {
        $flattenQueryParams = [];
        array_walk_recursive($this->queryParams, function($a) use (&$flattenQueryParams) {
            $flattenQueryParams[] = $a;
        });

        return $flattenQueryParams;
    }

    protected function escapeColumns(array $columns) {
        array_walk($columns, function(&$value) {
            $escapeColumn = function($value, $noAutoAlias = false) {
                return preg_replace_callback('/(\*|\w+)(\))?$/i', function ($matches) use ($value, $noAutoAlias) {
                    if (!$noAutoAlias && isset($matches[2]) && $matches[2] === ')' && preg_match('/^(\w+)\((\w+)?/i', $value, $alias)) {
                        $alias = ' AS ' . strtolower((isset($alias[2]) ? $alias[2] : $this->tableAlias).'_'.$alias[1]);
                    }
                    return sprintf('%s%s%s', $matches[1] === '*' ? '*' : "`$matches[1]`", isset($matches[2]) ? $matches[2] : '', isset($alias) ? $alias : '');
                }, $value);
            };
            if (preg_match('/^(\w+(?:(?:\((?:\*|\w+(?:\.\w+))\)|\.\w+))?)\s+(?:[aA][sS]\s+)?(\w+)$/i', $value, $matches)) {
                $value = sprintf('%s AS %s', $escapeColumn($matches[1], true), $matches[2]);
            } else {
                $value = $escapeColumn($value);
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
                count($this->selects) ? implode(', ', $this->escapeColumns($this->selects)) : '*',
                $this->tableName,
                $this->tableAlias
        );
    }

    public function where(array $where, $operator = 'AND')
    {
        $collection = [];
        foreach ($where as $key => $value) {
            if (!isset($value)) $collection[$key] = [];
            if (is_array($value) && count($value) > 1) {
                foreach ($value as $val) {
                    $collection[$key][] = $val;
                }
            } else {
                $collection[$key][] = $value;
            }
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
        $this->queryParams = [];

        if (count($this->wheres)) {
            $wheres = array_combine($this->escapeColumns(array_keys($this->wheres)), $this->wheres);

            $whereList = [];
            foreach ($wheres as $where) {
                $currentWhereCollection = [];
                foreach ($where['collection'] as $key => &$collection) {
                    foreach ($collection as &$value) {
                        switch (true) {
                            case is_array($value):
                                $operator = key($value);
                                $value = $value[$operator];
                                break;
                            case $value === null:
                                $operator = 'IS';
                                break;
                            default:
                                $operator = '=';
                                break;
                        }
                        $currentWhereCollection[] = sprintf('%s %s %s',
                            $key,
                            $operator,
                            strtoupper($operator) === 'IN' ? '('.implode(', ', array_fill(0, count($value), '?')).')' : '?')
                        ;
                    }
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
        $this->limit = sprintf(' LIMIT %s', $end === null ? $start : "$start, $end");

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

        return $this->db->fetchAll($this->getQuery(), $this->getQueryParams());
    }

    public function fetchAllKeyPair()
    {
        return $this->db->executeQuery($this->getQuery(), $this->getQueryParams())->fetchAll(\PDO::FETCH_KEY_PAIR);
    }

    public function fetchAllKeyPairs()
    {
        $results = $this->db->executeQuery($this->getQuery(), $this->getQueryParams())->fetchAll(\PDO::FETCH_GROUP|\PDO::FETCH_ASSOC);

        return array_map('reset', $results);
    }

    public function fetchAssoc(array $where = array())
    {
        if (count($where)) $this->where($where);

        return $this->db->fetchAssoc($this->getQuery(), $this->getQueryParams());
    }

    public function executeQuery($query, array $queryParams = array())
    {
        if (count($queryParams) === 0) {
            $queryParams = $this->getQueryParams();
        }

        return $this->db->executeQuery($query, $queryParams);
    }

    private function setTimestampsForData(&$data)
    {
        if ($this->isTimestampable) {
            $now = get_object_vars(new \DateTime('now'))['date'];
            $data['created_at'] = $now;
            $data['updated_at'] = $now;
        }
    }

    public function insert(array $data, $returnAffectedRowsCount = false)
    {
        $this->setTimestampsForData($data);
        $affectedRowsCount = $this->db->insert($this->tableName, $this->cleanData($data));

        return $returnAffectedRowsCount ? $affectedRowsCount : $this->db->lastInsertId();
    }

    public function insertIgnore(array $data, $returnAffectedRowsCount = false)
    {
        $this->setTimestampsForData($data);
        $affectedRowsCount = $this->db->executeUpdate(
            sprintf('INSERT %s INTO %s (%s) VALUES (%s)',
                $this->db->getParams()['driver'] === 'pdo_sqlite' ? 'OR IGNORE' : 'IGNORE',
                $this->tableName,
                implode(', ', array_keys($data)),
                implode(', ', array_fill(0, count($data), '?'))
            ),
            array_values($data)
        );

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
