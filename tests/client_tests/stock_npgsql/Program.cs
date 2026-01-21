using System.Collections.Generic;
using System;
using System.Diagnostics;
using Npgsql;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using System.Data;

namespace stock_npgsql
{
    public class TestEntity
    {
        public String Id { get; set; }
        public DateTimeOffset DateTime{ get; set; } = DateTimeOffset.UtcNow;

        public TestEntity() {
            Id = Guid.NewGuid().ToString();
        }
    }

    public class CrateContext : DbContext
    {
          public virtual DbSet<TestEntity> Test { get; set; }
          public static string ConnectionString { get; set; }

          public CrateContext() { }

          public CrateContext(DbContextOptions<CrateContext> options) : base(options) { }

          protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
          {
              optionsBuilder.UseNpgsql(ConnectionString);
          }

          protected override void OnModelCreating(ModelBuilder modelBuilder)
          {
              modelBuilder.Entity<TestEntity>(entity =>
              {
                  entity.HasKey(e => e.Id);
                  entity.ToTable("test", "test");
                  entity.Property(e => e.Id).HasColumnName("id").HasColumnType("text");
                  entity.Property(e => e.DateTime).HasColumnName("datetime").HasColumnType("timestamp with time zone");
              });
          }
    }

    class Program
    {
        public static async Task TestUnnestAsync(NpgsqlConnection conn)
        {
            using (var cmd = new NpgsqlCommand("DROP TABLE IF EXISTS mm.data_table", conn)) {
                cmd.ExecuteNonQuery();
            }
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

                // await TestBasics(conn);
                // await TestUnnestAsync(conn);
                // await TestInsertUsingEntityFramework(conn);
                await TestInsertWithDuplicateKeyConflict(conn);
            }
        }

        private static async Task TestInsertWithDuplicateKeyConflict(NpgsqlConnection conn)
        {
            using (var cmd = new NpgsqlCommand("drop table if exists test.test", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }
            string createTable = @"
                CREATE TABLE test.test (
                    id text default gen_random_text_uuid() primary key,
                    datetime timestamp with time zone
                )";
            using (var cmd = new NpgsqlCommand(createTable, conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }
            CrateContext.ConnectionString = conn.ConnectionString;
            var entries = new List<TestEntity>();
            using (CrateContext context = new()) {
                for (int i = 0; i < 4; i++)
                {
                    var entry = new TestEntity();
                    context.Test.Add(entry);
                    entries.Add(entry);
                }
                await context.SaveChangesAsync();
            }

            using (CrateContext context = new()) {
                foreach (var entry in entries) {
                    context.Test.Add(entry);
                }
                await context.SaveChangesAsync();
            }
        }

        private static async Task TestBasics(NpgsqlConnection conn)
        {
            using (var cmd = new NpgsqlCommand("SELECT mountain FROM sys.summits ORDER BY 1 LIMIT 10", conn))
            using (var reader = cmd.ExecuteReader()) {
                List<string> mountains = new List<string>();
                while (reader.Read()) {
                    mountains.Add(reader.GetString(0));
                }
                Debug.Assert(mountains[0] == "Acherkogel");
            }

            using (var cmd = new NpgsqlCommand("DROP TABLE IF EXISTS tbl", conn)) {
                cmd.ExecuteNonQuery();
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
                        await cmd.ExecuteNonQueryAsync();
                    }

                    await transaction.CommitAsync();
                }
            }
            using (var cmd = new NpgsqlCommand("REFRESH TABLE tbl", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            using (var cmd = new NpgsqlCommand("SELECT x FROM tbl ORDER BY 1 ASC LIMIT 10", conn))
            using (var reader = cmd.ExecuteReader())
            {
                Debug.Assert(reader.Read());
                int value = (int) reader[0];
                Debug.Assert(value == 10, "first value must be 10, but is " + value);
            }
        }

        private static async Task TestInsertUsingEntityFramework(NpgsqlConnection conn)
        {
            using (var cmd = new NpgsqlCommand("DROP TABLE IF EXISTS test.test", conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }
            string createTable = @"
                CREATE TABLE test.test (
                    id text default gen_random_text_uuid() primary key,
                    datetime timestamp with time zone
                )";
            using (var cmd = new NpgsqlCommand(createTable, conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }
            CrateContext.ConnectionString = conn.ConnectionString;
            using CrateContext context = new();

            for (int i = 0; i < 4; i++)
            {
                context.Test.Add(new TestEntity());
            }
            await context.SaveChangesAsync();
        }
    }
}
