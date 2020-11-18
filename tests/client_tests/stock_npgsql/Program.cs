using System.Collections.Generic;
using System;
using System.Diagnostics;
using Npgsql;
using System.Threading.Tasks;
using System.Linq;

namespace stock_npgsql
{
    class Program
    {

        public static async Task TestUnnestAsync(NpgsqlConnection conn)
        {
            using var command = new NpgsqlCommand(
                connection: conn,
                cmdText: "CREATE TABLE mm.data_table (id int, name text)"
            );
            await command.ExecuteNonQueryAsync();

            var records = Enumerable
                .Range(0, 10)
                .Select(i => (Id: i, Name: $"My identifier is {i}"))
                .ToArray();

            command.CommandText = "INSERT INTO mm.data_table (id, name) SELECT * FROM unnest(@i, @n) AS d";

            command.Parameters.Add(new NpgsqlParameter<int[]>("i", records.Select(e => e.Id).ToArray()));
            command.Parameters.Add(new NpgsqlParameter<string[]>("n", records.Select(e => e.Name).ToArray()));

            await command.ExecuteNonQueryAsync();
        }


        static async Task Main(string[] args)
        {
            string host = args[0]; string port = args[1];

            var connString = $"Host={host};Port={port};Username=crate;Password=;Database=doc";
            using (var conn = new NpgsqlConnection(connString)) {
                conn.Open();
                using (var cmd = new NpgsqlCommand("SELECT mountain FROM sys.summits ORDER BY 1 LIMIT 10", conn))
                using (var reader = cmd.ExecuteReader()) {
                    List<string> mountains = new List<string>();
                    while (reader.Read()) {
                        mountains.Add(reader.GetString(0));
                    }
                    Debug.Assert(mountains[0] == "Acherkogel");
                }

                using (var cmd = new NpgsqlCommand("CREATE TABLE tbl (x int)", conn)) {
                    cmd.ExecuteNonQuery();
                }
                using (var cmd = new NpgsqlCommand("INSERT INTO tbl (x) VALUES (@x)", conn))
                {
                    cmd.Parameters.AddWithValue("x", 10);
                    cmd.ExecuteNonQuery();
                }
                using (var cmd = new NpgsqlCommand("INSERT INTO tbl (x) VALUES (@x)", conn))
                {
                    using (var transaction = conn.BeginTransaction())
                    {
                        cmd.Transaction = transaction;
                        cmd.Parameters.Add("@x", NpgsqlTypes.NpgsqlDbType.Integer);

                        for (int i = 1; i < 10; i++)
                        {
                            cmd.Parameters["@x"].Value = i * 10;
                            cmd.ExecuteNonQuery();
                        }

                        transaction.Commit();
                    }
                }
                using (var cmd = new NpgsqlCommand("REFRESH TABLE tbl", conn))
                {
                    cmd.ExecuteNonQuery();
                }

                using (var cmd = new NpgsqlCommand("SELECT x FROM tbl ORDER BY 1 ASC LIMIT 10", conn))
                using (var reader = cmd.ExecuteReader())
                {
                    Debug.Assert(reader.Read());
                    int value = (int) reader[0];
                    Debug.Assert(value == 10, "first value must be 10, but is " + value);
                }

                await TestUnnestAsync(conn);
            }
        }
    }
}
