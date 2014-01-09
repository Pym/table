<?php
namespace Pym;

use Doctrine\DBAL\Connection;
use Pym\Table;

class TableEmbed
{
    protected $db = null;
    protected $table = null;
    protected $embeddedClassName = '';
    protected $embeddedTableName = '';
    protected $embeddedAliasName = '';
    protected $fields = [];

    public function __construct(Connection $db, Table $table)
    {
        $this->db = $db;
        $this->table = $table;
    }

    protected function getTableName($tableName) {
        return explode(' ', $tableName)[0];
    }

    protected function getTableAlias($tableName) {
        return explode(' ', $tableName)[1];
    }

    public function add($embeddedClassName, $embeds, array $fields) {
        $this->embeddedClassName = $embeddedClassName;
        $this->fields = $fields;

        $tableName = $this->table->getTableName();
        $tableAlias = $this->table->getTableNameOrAlias();

        if (is_array($embeds)) {
            $jointTable = $embeds[0];
            $embedTable = $embeds[1];

            $jointTableAlias = $this->getTableAlias($jointTable);

            $embedTableName = $this->getTableName($embedTable);
            $embedTableAlias = $this->getTableAlias($embedTable);

            $this->table = $this->table
                ->leftJoin($jointTable,
                    sprintf('%s.%s_id', $jointTableAlias, $tableName),
                    sprintf('%s.id', $tableAlias)
                )
                ->leftJoin($embedTable,
                    sprintf('%s.id', $embedTableAlias),
                    sprintf('%s.%s_id', $jointTableAlias, $embedTableName)
                );
        } else {
            $this->table = $this->table
                ->leftJoin($embeds,
                    sprintf('%s.%s_id', $jointTableAlias, $tableName),
                    sprintf('%s.id', $tableAlias)
                );
        }

        $this->embeddedTableName = $this->getTableName($embedTable);
        $this->embeddedAliasName = $this->getTableAlias($embedTable);

        return $this;
    }

    public function where(array $where) {
        $this->table = $this->table->where($where);
    }

    public function finish() {

        $this->table->select(array_merge([$this->table->getTableNameOrAlias().'.*'], array_map(function($value) {
            return sprintf('%s.%s %s_%s', $this->embeddedAliasName, $value, $this->embeddedAliasName, $value);
        }, $this->fields)));

        $result = $this->table->executeQuery($this->table->getQuery())->fetch(\PDO::FETCH_NAMED);

        $prefix = $this->embeddedAliasName.'_';

        $prepareData = function($index = null) use (&$result, $prefix) {
            $object = [];
            foreach ($this->fields as $field) {
                if ($index === null) {
                    $object[$field] = $result[$prefix.$field];
                    unset($result[$prefix.$field]);
                } else {
                    $object[$field] = $result[$prefix.$field][$index];
                    unset($result[$prefix.$field][$index]);
                }
            }

            return $object;
        };

        $objects = [];
        $id_field = $prefix.$this->fields[0];
        if (is_array($result[$id_field])) {
            $count = count($result[$id_field]);
            for ($i = 0; $i < $count; $i++) {
                $collection[] = new $this->embeddedClassName($prepareData($i));
            }
        } else {
            $collection[] = new $this->embeddedClassName($prepareData());
        }

        $result[$this->embeddedTableName.'s'] = $collection;

        return $result;
    }

}
