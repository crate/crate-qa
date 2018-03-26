module Main where

import           Control.Monad            (forM_)
import qualified Database.HDBC            as DB
import           Database.HDBC.PostgreSQL (connectPostgreSQL)
import           System.Environment       (getArgs)
import           Text.Printf              (printf)


expectedRows :: [[DB.SqlValue]]
expectedRows = map (\i -> [DB.SqlInt32 i]) [1..10]

main :: IO ()
main = do
  [host, port] <- getArgs
  let
    dbUri = printf "host=%s dbname=doc user=crate port=%s" host port
  conn <- connectPostgreSQL dbUri
  DB.runRaw conn "drop table if exists t1"
  DB.runRaw conn "create table t1 (x int)"
  insertStmt <- DB.prepare conn "insert into t1 (x) values (?)" 
  DB.execute insertStmt [DB.SqlInt32 100]
  DB.executeMany insertStmt (map (\i -> [DB.SqlInt32 i]) [1..10])
  DB.runRaw conn "refresh table t1"
  DB.run conn "delete from t1 where x = ?" [DB.SqlInt32 100]
  DB.runRaw conn "refresh table t1"
  rows <- DB.quickQuery conn "select x from t1 order by x" []
  if rows /= expectedRows
    then fail "rows didn't match expected result"
    else forM_ rows print
