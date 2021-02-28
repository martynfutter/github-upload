using System;
using Microsoft.Data.Sqlite;
using System.Text;
using System.IO;

namespace TransactionTest
{
	public static class textFileImport
	{
		public static void import(int fieldCount)
		{
            string tableName = "results";

            string nullPlaceHolder = "x";

            using (var connection = new SqliteConnection("Data Source=hello.db"))
            {
                connection.Open();

                var dropTableCommand = new StringBuilder("DROP TABLE IF EXISTS ").Append(tableName);
                var dropTable = new SqliteCommand(dropTableCommand.ToString(), connection);
                dropTable.ExecuteNonQuery();

                var createTableCommand = new StringBuilder("CREATE TABLE ").Append(tableName).Append(" (tmp_0 NVARCHAR(254) NULL ");

                for (int i = 1; i < fieldCount; i++)
                {
                    createTableCommand.Append(", tmp_").Append(i.ToString()).Append(" NVARCHAR(254) NULL");
                }
                createTableCommand.Append(")");

                Console.WriteLine(createTableCommand.ToString());

                var createTable = new SqliteCommand(createTableCommand.ToString(), connection);

                createTable.ExecuteReader();

                Console.WriteLine(System.DateTime.Now.ToString());

                using (var transaction = connection.BeginTransaction())
                {
                    var command = connection.CreateCommand();

                    var insertCommand = new StringBuilder(@"INSERT INTO ").Append(tableName).Append(" VALUES ($value_0 ");
                    for (int i = 1; i < fieldCount; i++)
                    {
                        insertCommand.Append(", $value_").Append(i.ToString());
                    }
                    insertCommand.Append(")");

                    command.CommandText = insertCommand.ToString();
                    Console.WriteLine(insertCommand.ToString());

                    for (int i = 0; i < fieldCount; i++)
                    {
                        var parameter = command.CreateParameter();
                        parameter.ParameterName = string.Concat("$value_", i.ToString());
                        command.Parameters.Add(parameter);
                    }


                    using (StreamReader sr = new StreamReader("results0.txt"))
                    {
                        string line;
                        while ((line = sr.ReadLine()) != null)
                        {
                            string[] fields = line.Split(',');
                            int elements = (fields.Length < fieldCount ? fields.Length : fieldCount);
                            for (int j = 0; j < elements; j++)
                            {
                                command.Parameters[j].Value = fields[j];
                            }
                            for (int j = elements; j < fieldCount; j++)
                            {
                                command.Parameters[j].Value = nullPlaceHolder;
                            }
                            command.ExecuteNonQuery();
                        }
                    }
                    transaction.Commit();
                }
                Console.WriteLine(System.DateTime.Now.ToString());
                connection.Close();
            }
        }
    }
}
