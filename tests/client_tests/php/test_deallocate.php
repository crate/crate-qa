#!/usr/bin/env php
<?php

if ($argc != 3) {
	echo "Provide both host and port of CrateDB";
	exit(1);
} 
$host = $argv[1];
$port = $argv[2];

if (empty($host) || empty($port)) {
    echo "Provide both host and port of CrateDB";
	exit(1);
}

$pdo = new PDO('pgsql:dbname=doc;user=crate;host='.$host.';port='.$port);
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

$stmt = $pdo->query("select name from sys.cluster");
while ($row = $stmt->fetch()) {
	echo "Cluster name: ".$row['name']."\n";
}

$stmt = $pdo->prepare("create table t1 (x int)");
$stmt->execute();

$stmt = $pdo->prepare("insert into t1 (x) values (?)");
$stmt->execute([1]);

$stmt = $pdo->prepare("insert into t1 (x) values (:val)");
$stmt->bindValue(':val', 2);
$stmt->execute();

$pdo->prepare("refresh table t1")->execute();
$stmt = NULL;

$count = $pdo->query("select count(x) from t1")->fetch()['count(x)'];
if ($count != 2) {
	echo "Wrong result:{$count}, expected: 2";
	exit(1);
}

?> 
