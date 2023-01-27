using System;
using System.Data;
using System.Text;
using MySql.Data.MySqlClient;

namespace PracticeSession1
{
    class Program
    {
        static string connectionString = "server=localhost; port=3306; uid=smitesh;" +
                                        "pwd=root; database=userdetail; charset=utf8; sslMode=none;";
        static MySqlConnection sqlConnection = new MySqlConnection(connectionString);
        static int start,end;
        static bool tmp = true;
        static int count = 0;
        static bool done = false;

        static void Main(String[] args)
        {

            Console.WriteLine("Connect to MySql:");

            using (sqlConnection)
            {
                try
                {
                    // open connection
                    sqlConnection.Open();
                    Console.WriteLine("Connection is " + sqlConnection.State.ToString() + Environment.NewLine);


                    // INSERT 1M record
                    // insertOneMillionRecord();

                    tuncateTable("destinationtable");

                    while(true)
                    {
                        Console.WriteLine("Do you want to migrat record? y/n");
                        string ss = Console.ReadLine();
                        if( ss == "y"){
                            count = 0;
                        }else if(ss == "n"){
                            break;
                        }else{
                            continue;
                        }

                        Console.WriteLine("Enter starting and ending number for migration: ");
                        start = int.Parse(Console.ReadLine());
                        end = int.Parse(Console.ReadLine());

                        Thread thread = new Thread(()=>migrateInRange(start,end));
                        thread.Start();

                        while(true){
                            if(done == true){
                                done = false;
                                break;
                            } 
                            string str = Console.ReadLine();
                            if(str == "cancel"){
                                Console.WriteLine($"{count} records are inserted.");
                                Console.WriteLine("Migration is canceled.");
                                tmp = false;
                                thread.Join();
                                break;
                            }
                            else if(str == "status"){
                                if(tmp == true){
                                    Console.WriteLine($"{count} migration ocuured.");
                                }else if(tmp == false){
                                    Console.WriteLine($"migration canceled.");
                                }
                            }
                        }

                        if(tmp == false){
                            tmp = true;
                            count = 0;
                        }
                    }
                    
                    sqlConnection.Close();
                    Console.WriteLine("Connection is " + sqlConnection.State.ToString() + Environment.NewLine);
                }
                catch (MySql.Data.MySqlClient.MySqlException ex)
                {
                    Console.WriteLine("Error: " + ex.Message.ToString());
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception: " + ex.Message);
                }
                finally
                {
                    Console.WriteLine("success fully run.");
                }
            }
        }

        private static void tuncateTable(string tableName)
        {
            MySqlCommand truncateCmd = new MySqlCommand($"TRUNCATE TABLE {tableName};",sqlConnection);
            truncateCmd.ExecuteNonQuery();
        }

        private static void insertOneMillionRecord()
        {
            Console.WriteLine("Records are inserting...");
            Random rnd = new Random();
            for(int j=0; j<10000;j++)
            {
                StringBuilder sCommand = new StringBuilder("INSERT INTO sourcetable (firstNumber, SecondNumber) VALUES ");
                List<string> Rows = new List<string>();
                for (int i = 0; i < 100; i++)
                {
                    Rows.Add(string.Format("('{0}','{1}')", rnd.Next(10000), rnd.Next(10000)));
                }

                sCommand.Append(string.Join(",", Rows));
                sCommand.Append(";");
                using (MySqlCommand myCmd = new MySqlCommand(sCommand.ToString(), sqlConnection))
                {
                    myCmd.CommandType = System.Data.CommandType.Text;
                    myCmd.ExecuteNonQuery();
                }
                Rows.Clear();
                sCommand.Clear();
            }
            Console.WriteLine("Records are inserted");
        }

        private static void migrateInRange(int n, int m)
        {
            try{
                Console.WriteLine("Fetching data...");

                MySqlCommand command2 = sqlConnection.CreateCommand();
                command2.CommandText = $"select * from sourcetable where ID >= {n} and ID <= {m}";
                MySqlDataReader read =  command2.ExecuteReader();

                List<string> Rows = new List<string>();
                while(read.Read()){
                    if(tmp == false){
                        Console.WriteLine("Migration is not started.");
                        break;
                    }
                    Rows.Add(string.Format("('{0}','{1}')", read.GetInt32(0), sum(read.GetInt32(1), read.GetInt32(2))));
                }

                read.Close();

                Console.WriteLine("recordes are inserting...");
                string sCommand = "INSERT INTO destinationtable (userId, sum) VALUES ";
                List<string> lst = new List<string>();
                for(int i=0; i<Rows.Count; i++)
                {
                    if(tmp == false) break;
                    if(i%100 != 0)
                    {
                        lst.Add(Rows[i].ToString());
                    }
                    else if(i%100 == 0)
                    {
                        lst.Add(Rows[i].ToString());
                        executeQuery(sCommand, lst);
                        count=i;
                        Thread.Sleep(3000);
                        
                        lst.Clear();
                    
                        sCommand=("INSERT INTO destinationtable (userId, sum) VALUES ");
                    }
                    
                }
                if (tmp == false) return;
                executeQuery(sCommand, lst);

                count = Rows.Count;
                Console.WriteLine("All records are inserted.");
                done = true;
                Console.WriteLine("press any key to continue..");
            }
            catch(Exception ex){
                Console.WriteLine("Exception");
            }
        }

        private static void executeQuery(string sCommand,List<string> lst)
        {
            sCommand +=(string.Join(",",lst)).ToString();
            sCommand+=(";");

            MySqlCommand myCmd1 = new MySqlCommand(sCommand, sqlConnection);
            myCmd1.CommandType = System.Data.CommandType.Text;
            myCmd1.ExecuteNonQuery();
        }

        private static int sum(int v1, int v2)
        {
            Thread.Sleep(50);
            return v1+v2;
        }

    }
}