using System.Collections.Generic;
using System;
using System.Diagnostics;
using Npgsql;

namespace stock_npgsql
{
    class Program
    {
        static void Main(string[] args)
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
                using (var cmd = new NpgsqlCommand("REFRESH TABLE tbl", conn)) {
                    cmd.ExecuteNonQuery();
                }
                using (var cmd = new NpgsqlCommand("SELECT x FROM tbl ORDER BY 1 LIMIT 10", conn))
                using (var reader = cmd.ExecuteReader()) {
                    Debug.Assert(reader.Read());
                    Debug.Assert((int)reader[0] == 10);
                }
            }
        }
    }
}
