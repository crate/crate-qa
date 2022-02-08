{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Exception.Base     (finally)
import           Control.Monad              (forM_)
import qualified Data.ByteString.Char8      as BS
import           Data.Functor.Contravariant (contramap)
import qualified Database.HDBC              as DB
import           Database.HDBC.PostgreSQL   (connectPostgreSQL)
import qualified Hasql.Connection           as Hasql
import qualified Hasql.Decoders             as Decoders
import qualified Hasql.Encoders             as Encoders
import qualified Hasql.Session              as Hasql
import qualified Hasql.Statement            as Hasql
import           System.Environment         (getArgs)
import           Text.Printf                (printf)


expectedRows :: [[DB.SqlValue]]
expectedRows = map (\i -> [DB.SqlInt32 i]) [1..10]


runQueriesWithHDBC :: String -> String -> IO ()
runQueriesWithHDBC host port = do
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


runQueriesWithHasqlConn :: Hasql.Connection -> IO ()
runQueriesWithHasqlConn conn = do
  run (Hasql.sql "drop table if exists t1")
  run (Hasql.sql "create table t1 (x int not null, name string not null)")
  rowCount <- run (Hasql.statement (10, "foo") insert)
  print rowCount
  run (Hasql.sql "refresh table t1")
  rows <- run (Hasql.statement () select)
  forM_ rows print
  where
    run = getRight <$> flip Hasql.run conn
    getRight f = do
      result <- f
      case result of
        Left err -> error $ show err
        Right val -> pure val
    insert = Hasql.Statement
      "insert into t1 (x, name) values ($1, $2)" insertParams Decoders.rowsAffected True
    insertParams =
      contramap fst (Encoders.param $ Encoders.nonNullable Encoders.int4) <>
      contramap snd (Encoders.param $ Encoders.nonNullable Encoders.text)
    intAndTextTuple = Decoders.rowList $
      (,) <$> (Decoders.column . Decoders.nonNullable) Decoders.int4
          <*> (Decoders.column . Decoders.nonNullable) Decoders.text
    select = Hasql.Statement "select x, name from t1" Encoders.noParams intAndTextTuple True


runQueriesWithHasql :: String -> String -> IO ()
runQueriesWithHasql host port = do
  errorOrConn <- Hasql.acquire settings
  case errorOrConn of
    Left err -> error $ show err
    Right conn ->
      runQueriesWithHasqlConn conn `finally` Hasql.release conn
  where
    settings = Hasql.settings (BS.pack host) (read port) user pw db
    user = "crate"
    pw = ""
    db = "hasql"


main :: IO ()
main = do
  [host, port] <- getArgs
  runQueriesWithHDBC host port
  runQueriesWithHasql host port
