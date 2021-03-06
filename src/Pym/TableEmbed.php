<?php
namespace Pym;

use Pym\Table;

class TableEmbed
{
    protected $table = null;
    protected $embeddedTables = [];

    public function __construct(Table $table)
    {
        $this->table = $table;
    }

    protected function getTableName($tableName) {
        return explode(' ', $tableName)[0];
    }

    protected function getTableAlias($tableName) {
        return explode(' ', $tableName)[1];
    }

    public function add($embeddedClassName, $embeds, array $fields) {
        $newEmbeddedTable = [];
        $newEmbeddedTable['class_name'] = $embeddedClassName;
        $newEmbeddedTable['fields'] = $fields;

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

        $newEmbeddedTable['table_name'] = $this->getTableName($embedTable);
        $newEmbeddedTable['alias_name'] = $this->getTableAlias($embedTable);

        $this->embeddedTables[] = $newEmbeddedTable;

        return $this;
    }

    public function where(array $where) {
        $this->table = $this->table->where($where);
    }

    public function finish() {
        $columns = [];
        foreach ($this->embeddedTables as $table) {
            foreach ($table['fields'] as $field) {
                $columns[] = sprintf('%s.%s %s_%s', $table['alias_name'], $field, $table['alias_name'], $field);
            }
        }

        $this->table->select(array_merge([$this->table->getTableNameOrAlias().'.*'], $columns));
        $result = $this->table->executeQuery($this->table->getQuery())->fetch(\PDO::FETCH_NAMED);

        $prepareData = function($prefix, $index = null) use (&$result, &$table) {
            $object = [];
            foreach ($table['fields'] as $field) {
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

        foreach ($this->embeddedTables as $table) {
            $collection = [];
            $prefix = $table['alias_name'].'_';
            $id_field = $prefix.$table['fields'][0];
            if ($result[$id_field] !== null) {
                if (is_array($result[$id_field])) {
                    $count = count($result[$id_field]);
                    for ($i = 0; $i < $count; $i++) {
                        $collection[] = new $table['class_name']($prepareData($prefix, $i));
                    }
                } else {
                    $collection[] = new $table['class_name']($prepareData($prefix));
                }
                $result[$table['table_name'].'s'] = $collection;
            }
        }

        return $result;
    }
}
