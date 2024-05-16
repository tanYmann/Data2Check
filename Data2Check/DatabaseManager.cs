using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace Data2Checker
{
    class DatabaseManager
    {
        private OdbcConnection connection;

        public DatabaseManager(string connectionString)
        {
            connection = new OdbcConnection(connectionString);
        }

        public void OpenConnection()
        {
            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }
        }

        public void CloseConnection()
        {
            if (connection.State == ConnectionState.Open)
            {
                connection.Close();
            }
        }

        public OdbcDataReader ExecuteReader(string query)
        {
            try
            {
                OpenConnection();

                using (OdbcCommand command = new OdbcCommand(query, connection))
                {
                    return command.ExecuteReader();
                } 
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error executing reader: {ex.Message}");
                throw;
            }
            finally
            {
                CloseConnection();
            }
        }
    }
}
