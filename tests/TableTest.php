<?php

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\PDOMySql\Driver;
use Pym\Table;

class TableTest extends PHPUnit_Framework_TestCase
{
    protected $testTable;

    public function __construct()
    {
        $this->testTable = new Table(null, 'user', 'u', ['u', 'c', 'i']);
    }

    public function testEmptySelect()
    {
        $this->testTable->select([]);

        $this->assertEquals($this->testTable->getQuery(), 'SELECT * FROM `user` u');
    }

    public function testSelect()
    {
        $this->testTable->select(['name', 'username']);

        $this->assertEquals($this->testTable->getQuery(), 'SELECT `name`, `username` FROM `user` u');
    }

    public function testSelectWithLeftJoin()
    {
        $this->testTable
            ->select(['u.name', 'u.username', 'c.name'])
            ->leftJoin('city c', 'c.id', 'u.city_id');

        $this->assertEquals($this->testTable->getQuery(),
            'SELECT u.`name`, u.`username`, c.`name` FROM `user` u LEFT JOIN `city` c ON c.id = u.city_id');
    }

    public function testWhere()
    {
        $this->testTable->where(['name' => 'foo bar', 'username' => 'foo']);

        $this->assertEquals($this->testTable->getQuery(), 'SELECT * FROM `user` u WHERE name = ? AND username = ?');
    }

    public function testOrWhere()
    {
        $this->testTable->orWhere(['name' => 'foo bar', 'username' => 'foo']);

        $this->assertEquals($this->testTable->getQuery(), 'SELECT * FROM `user` u WHERE (name = ? OR username = ?)');
    }

    public function testWhereLike()
    {
        $this->testTable->where(['name' => 'foo bar', 'username' => ['LIKE' => '"foo%"']]);

        $this->assertEquals($this->testTable->getQuery(), 'SELECT * FROM `user` u WHERE name = ? AND username LIKE ?');
    }

    public function testLimit()
    {
        $this->testTable->limit(10);

        $this->assertEquals($this->testTable->getQuery(), 'SELECT * FROM `user` u LIMIT 10');
    }

    public function testLimit2()
    {
        $this->testTable->limit(0, 10);

        $this->assertEquals($this->testTable->getQuery(), 'SELECT * FROM `user` u LIMIT 0, 10');
    }

    public function testCount()
    {
        $this->testTable->count();

        $this->assertEquals($this->testTable->getQuery(), 'SELECT COUNT(*) AS u_count FROM `user` u');
    }

    public function testSelectCount()
    {
        $this->testTable
            ->select(['u.username', 'COUNT(i.id)'])
            ->leftJoin('item i', 'i.owner_id', 'u.id');

        $this->assertEquals($this->testTable->getQuery(), 'SELECT u.`username`, COUNT(i.`id`) AS i_count FROM `user` u LEFT JOIN `item` i ON i.owner_id = u.id');
    }

}
