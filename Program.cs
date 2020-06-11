using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using Oracle.DataAccess.Client;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;

namespace ConsoleApp13
{
    
    public class Program
    {
        //private static string sql;

        static void Main()
        {
            while (true)
            {
                Console.WriteLine("Пункт меню");
                Console.WriteLine("1 Обновить поля STATUS и WAITINGFOREXPORT");
                Console.WriteLine("2 Экспорт в csv");
                Console.WriteLine("3 Генерация новых данных в Oracle \n");


                int chose = Convert.ToInt32(Console.ReadLine()); ;

                if (chose == 1)
                {
                    getcount();
                    updateBD();
                }
                else if (chose == 2)
                {
                    exporttocsv();
                }
                else if (chose == 3)
                {
                    generatenewdata();
                }

                else  
                {
                    continue;
                }

            }
            
        }

        private static void generatenewdata()
        {
            OracleConnection conn = DBUtils.GetDBConnection();
            
            OracleCommand cmd = new OracleCommand();
            
           

            int count = 1;
            int ID = 2000011;

            while (count<=1000000)
           {
                
                string sql = "INSERT INTO CARDSTATUS1 (HASH,END_DATE,STATUS,WAITINGFOREXPORT,ID) VALUES('HELLO', '09.06.2020', 'ACTIVE', ''," + ID + ")";
                cmd.Connection = conn;
                cmd.CommandText = sql;

                conn.Open();
                cmd.ExecuteNonQuery();
                ID=ID+1;
                count = count + 1;
                conn.Close();
            }

            Console.WriteLine("Данные сгененированы" );

        }

        private static void updateBD()
        {
            DateTime todayDate = DateTime.Now;
            //Console.WriteLine("Текущая дата = " + todayDate.ToShortDateString());
            OracleConnection conn = DBUtils.GetDBConnection();
            string sql = "SELECT HASH,END_DATE,STATUS,WAITINGFOREXPORT,ID from CARDSTATUS1";
            OracleCommand cmd = new OracleCommand();
            cmd.Connection = conn;
            cmd.CommandText = sql;

            try
            {
                conn.Open();
                Console.WriteLine("Соединение прошло успешно к " + conn.ConnectionString);

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string HASH = Convert.ToString(reader.GetValue(0));
                        DateTime END_DATE = Convert.ToDateTime(reader.GetValue(1));
                        string STATUS = Convert.ToString(reader.GetValue(2));
                        string WAITINGFOREXPORT = Convert.ToString(reader.GetValue(3));
                        Int32 ID = Convert.ToInt32(reader.GetValue(4));
                        Double timeleft = Convert.ToDouble((END_DATE - todayDate).TotalDays);
                        //Console.WriteLine("" + ID + " " + HASH + " " + END_DATE.ToShortDateString() + " " + STATUS + " " + WAITINGFOREXPORT + " " + timeleft);

                        //Должны обновить поле STATUS и WAITINGFOREXPORT
                        if (timeleft < 0)
                        {

                            // OracleCommand sqlUpdate = new OracleCommand("UPDATE CARDSTATUS1 SET STATUS = 'EXPIRED', WAITINGFOREXPORT='1' WHERE ID = " + ID + "", conn);
                            //sqlUpdate.ExecuteNonQuery();
                            string sql11 = "UPDATE CARDSTATUS1 SET STATUS = 'EXPIRED', WAITINGFOREXPORT='1' WHERE ID = " + ID + "";
                            OracleCommand cmd1 = new OracleCommand();
                            cmd1.Connection = conn;
                            cmd1.CommandText = sql11;
                            cmd1.ExecuteNonQuery();

                        }
                    }

                }

                Console.WriteLine("STATUS и WAITINGFOREXPOR обновлен\n");
                conn.Close();


            }
            catch (Exception ex)
            {
                Console.WriteLine("## ERROR: " + ex.Message);
                Console.Read();
                return;
            }
        }

        private static async void exporttocsv()
        {
            OracleConnection conn = DBUtils.GetDBConnection();
            conn.Open();

            try
            {
                Console.WriteLine("Переводим данные в CSV");
                string sql = "SELECT * FROM CARDSTATUS1 WHERE WAITINGFOREXPORT='1'";
                OracleCommand cmd = new OracleCommand();
                cmd.Connection = conn;
                cmd.CommandText = sql;

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string HASH = Convert.ToString(reader.GetValue(0));
                        string END_DATE = Convert.ToString(reader.GetValue(1));
                        string STATUS = Convert.ToString(reader.GetValue(2));
                        Int32 ID = Convert.ToInt32(reader.GetValue(4));
                        string WAITINGFOREXPORT = Convert.ToString(reader.GetValue(3));
                        //Console.WriteLine( ID + " " + HASH + " " + END_DATE + " " + STATUS + " ");
                        using (StreamWriter sw = new StreamWriter(@"C:\Downloads\1.csv", true, Encoding.Default))
                        {
                            await sw.WriteLineAsync( ID + " "+ HASH + " " + END_DATE + " " +  STATUS);
                        }
                    }
                }

                Console.WriteLine("Данные скачались в файл C:\\Downloads\\1.csv");
                conn.Close();
                Console.ReadLine();
                
            }
            catch (Exception ex)
            {
                Console.WriteLine("## ERROR: " + ex.Message);
                Console.Read();
                
            }
        }

        static Double getcount()
        {

            OracleConnection conn = DBUtils.GetDBConnection();
            string sql = "SELECT COUNT(*) from CARDSTATUS1";
            OracleCommand cmd = new OracleCommand();
            cmd.Connection = conn;
            cmd.CommandText = sql;
            conn.Open();
            try

            {
                Console.WriteLine("Количество строк удалось получить");
                double countrows = Convert.ToInt32(cmd.ExecuteScalar());
                Console.WriteLine("Count = " + countrows);
                return countrows;

                conn.Close();

            }
            catch (Exception ex)
            {
                Console.WriteLine("## ERROR: " + ex.Message);
                Console.Read();
                return 0;
            }


        }
    }
}
