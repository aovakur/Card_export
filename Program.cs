using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.Common;
using Oracle.DataAccess.Client;
using System.Data.SqlClient;
using System.IO;

using System.Threading;

namespace ConsoleApp13
{
    
    public class Program
    {
        private static int i;

        //private static string sql;

        static void Main()
        {

            while (true)
            {
                Console.WriteLine("Пункт меню");
                Console.WriteLine("1 Обновить поля STATUS и WAITINGFOREXPORT");
                Console.WriteLine("2 Экспорт в csv");
                Console.WriteLine("3 Генерация новых данных в Oracle");
                Console.WriteLine("4 Получить общее количество данных");
                Console.WriteLine("5 Редактирование поля \n");


                int chose = Convert.ToInt32(Console.ReadLine());
                int count = 0;

                if (chose == 1)
                {

                    // Int32 count_BD = 0;
                    //count_BD = Convert.ToInt32(getcount());
                    //Console.WriteLine("Количество = " + count_BD);



                    //  while (count_BD > 500000)

                    //   {
                    // count_BD = count_BD - 500000;
                    // count = count + 1;
                    //  if (count_BD < 500000)
                    //  {
                    //      count = count + 1;
                    // }

                    // }

                    int[] nums = new int[4] { 1, 2, 3, 5 }; ;

                    //if (count == 3)
                    // {
                        Thread p1 = new Thread(new ParameterizedThreadStart(updateBD));
                        p1.IsBackground = true;
                        p1.Start(0);

                    // Фоновые потоки
                    Thread p2 = new Thread(new ParameterizedThreadStart(updateBD));
                    p1.IsBackground = true;
                    p2.Start(500000);
                    Thread p3 = new Thread(new ParameterizedThreadStart(updateBD));
                    p3.IsBackground = false;
                    p3.Start(1000000);
                    //}

                }
                else if (chose == 2)
                {
                    exporttocsv();
                }
                else if (chose == 3)
                {
                    generatenewdata();
                }
                else if (chose == 5)
                {
                    
                }
                else if (chose == 4)
                {

                    Int32 count_BD = 0;
                    count_BD = Convert.ToInt32(getcount());
                    Console.WriteLine("Количество = "+count_BD);

                    double countunderthreat = count_BD / 3;


                    while (count_BD > 500000)

                    {
                        count_BD = count_BD - 500000;
                        count = count + 1;
                        if (count_BD < 500000)
                        {
                            count = count + 1;
                        }

                    }
                    Console.WriteLine("Итог "+count + " Разделено на 3 " + countunderthreat);

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
            int ID = 9000011;

            while (count<=1000000)
           {
                
                string sql = "INSERT INTO CARDSTATUS1 (HASH,END_DATE,STATUS,WAITINGFOREXPORT,ID) VALUES('HELLO', '14.06.2020', 'ACTIVE', ''," + ID + ")";
                cmd.Connection = conn;
                cmd.CommandText = sql;

                conn.Open();
                cmd.ExecuteNonQuery();
                ID =ID+1;
                count = count + 1;
                conn.Close();
                //Console.WriteLine(ID);
            }

            Console.WriteLine("Данные сгененированы" );

        }

        

        public static void updateBD(Object START)
        {
            OracleConnection conn = DBUtils.GetDBConnection();
            conn.Open();
            int start = (int)START;
            DateTime todayDate = DateTime.Now;
            //Console.WriteLine("Текущая дата = " + todayDate.ToShortDateString());

            //string sql = "SELECT HASH,END_DATE,STATUS,WAITINGFOREXPORT,ID from CARDSTATUS1";
            //int finish = start + 500000-2;
            int finish = start + 499999;
            //int finish = start + 5;
            //Console.WriteLine("Старт"+start+ "Стоп" + finish + "\r");

            string sql = "select * FROM(SELECT ROWNUM rn, v.HASH, v.END_DATE,v.STATUS,v.WAITINGFOREXPORT,v.ID FROM CARDSTATUS1 v) WHERE rn >= "+start+ " AND rn <= " + finish + "";
            //string sql = "select* from(select a.*, rownum rnum from(SELECT HASH,END_DATE,STATUS,WAITINGFOREXPORT,ID from CARDSTATUS1) a where rownum <= "+finish+") where rnum >= "+start+"";

            OracleCommand cmd = new OracleCommand();
            cmd.Connection = conn;
            cmd.CommandText = sql;

            try
            {
                
                ///Console.WriteLine("Соединение прошло успешно к " + conn.ConnectionString);

                using (DbDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string rn = Convert.ToString(reader.GetValue(0));
                        string HASH = Convert.ToString(reader.GetValue(1));
                        DateTime END_DATE = Convert.ToDateTime(reader.GetValue(2));
                        string STATUS = Convert.ToString(reader.GetValue(3));
                        string WAITINGFOREXPORT = Convert.ToString(reader.GetValue(4));
                        int ID = Convert.ToInt32(reader.GetValue(5));
                        Double timeleft = Convert.ToDouble((END_DATE - todayDate).TotalDays);
                        //Console.WriteLine(rn+" " + ID + " " + HASH + " " + END_DATE.ToShortDateString() + " " + STATUS + " " + WAITINGFOREXPORT + " " + timeleft);

                        //string HASH = Convert.ToString(reader.GetValue(0));
                        //DateTime END_DATE = Convert.ToDateTime(reader.GetValue(1));
                        //string STATUS = Convert.ToString(reader.GetValue(2));
                        //string WAITINGFOREXPORT = Convert.ToString(reader.GetValue(3));
                        //Int32 ID = Convert.ToInt32(reader.GetValue(4));
                        //Double timeleft = Convert.ToDouble((END_DATE - todayDate).TotalDays);
                        //Console.WriteLine("" + ID + " " + HASH + " " + END_DATE.ToShortDateString() + " " + STATUS + " " + WAITINGFOREXPORT + " " + timeleft);

                        //Должны обновить поле STATUS и WAITINGFOREXPORT
                        if (timeleft < 0)
                        {
                            updateID(ID);

                        }
                    }

                }

                Console.WriteLine("STATUS и WAITINGFOREXPOR обновлен\n");
                conn.Close();
                //return 0;


            }
            catch (Exception ex)
            {
                Console.WriteLine("## ERROR: " + ex.Message);
                Console.Read();
                //return 0;
            }
            //Console.WriteLine();
        }

        public static void updateID(int ID)
        {
          
            //Console.WriteLine("Имеем проблему у " + ID + "");
            // OracleCommand sqlUpdate = new OracleCommand("UPDATE CARDSTATUS1 SET STATUS = 'EXPIRED', WAITINGFOREXPORT='1' WHERE ID = " + ID + "", conn);
            //sqlUpdate.ExecuteNonQuery();

            OracleConnection conn1 = DBUtils.GetDBConnection();
            conn1.Open();
            string sql11 = "UPDATE CARDSTATUS1 SET STATUS = 'EXPIRED', WAITINGFOREXPORT='1' WHERE ID = '" + ID + "'";
            OracleCommand cmd1 = new OracleCommand();
            cmd1.Connection = conn1;
            cmd1.CommandText = sql11;

            cmd1.ExecuteNonQuery();
            conn1.Close();
 
        }

        private static void exporttocsv()
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
                            sw.WriteLine( ID + " "+ HASH + " " + END_DATE + " " +  STATUS);
                            updatewaitingforexport(ID);
                            //Console.WriteLine("В файл записано" + ID);
                            //Thread p2 = new Thread(new ParameterizedThreadStart(updatewaitingforexport));
                            //  p2.IsBackground = true;
                            //p2.Start(ID);
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


        // Обновляем поле updatewaitingforexport
        private static void updatewaitingforexport(Object ID)
        {
            int ID_2 = (int)ID;
            OracleConnection conn3 = DBUtils.GetDBConnection();
            conn3.Open();
            string sqlupdatewaitingforexport = "UPDATE CARDSTATUS1 SET WAITINGFOREXPORT='' WHERE ID = '" + ID_2 + "'";
            OracleCommand cmd1 = new OracleCommand();
            cmd1.Connection = conn3;
            cmd1.CommandText = sqlupdatewaitingforexport;
            cmd1.ExecuteNonQuery();
            conn3.Close();
           //Console.WriteLine("Обновление произошло" + ID);
        }

        //Получаем общее количество записей
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
                //Console.WriteLine("Количество строк удалось получить");
                double countrows = Convert.ToInt32(cmd.ExecuteScalar());
                //Console.WriteLine("Count = " + countrows);
                //double step = countrows / 3;
                return countrows;

                //conn.Close();

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
