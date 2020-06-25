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
using System.Runtime.Remoting.Messaging;
using System.Security.Cryptography.X509Certificates;
using System.ComponentModel;
using System.Data;

namespace ConsoleApp13
{
    
    public class Program
    {
        //public delegate int UpdateID(int number);
        public delegate int Updatewaiting(int id);
        public delegate void WaitCallback(Object ID);

        //static AutoResetEvent waitHandler = new AutoResetEvent(true);

        static void Main()
        {

            while (true)
            {

                int workers = 10;
                int threads = 10;
                int workers1=0;
                int threads1=0;

                bool a = ThreadPool.SetMaxThreads(workers, threads);
                ThreadPool.GetMaxThreads(out workers1, out threads1);
                Console.WriteLine(a);
                Console.WriteLine("Потоков =" + workers1 +" потоков выводы "+ threads1);

                Console.WriteLine("Пункт меню");
                Console.WriteLine("1 Обновить поля STATUS и WAITINGFOREXPORT");
                Console.WriteLine("2 Экспорт в csv");
                Console.WriteLine("3 Генерация новых данных в Oracle");
                Console.WriteLine("4 Обновление и экспорт");



                int chose = Convert.ToInt32(Console.ReadLine());

                if (chose == 1)
                {
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
                else if (chose == 4)
                {
                    updateBD();
                    exporttocsv();
                }
                else  
                {
                    continue;
                }

            }
            
        }
        
       

        private static void generatenewdata()
        {

            int count = 1;
            int ID = 6000070;


            while (count<= 1000000)
            {
                OracleConnection conn = DBUtils.GetDBConnection();
                conn.Open();
                OracleCommand cmd = conn.CreateCommand();
                OracleTransaction transaction;
                transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                cmd.Transaction = transaction;
                try
                {
                    cmd.CommandText = "INSERT INTO CARDSTATUS4 (HASH,END_DATE,STATUS,WAITINGFOREXPORT,ID) VALUES('HELLO', to_date('20200610','YYYYMMDD'), 'ACTIVE',NULL," + ID + ")";
                    cmd.ExecuteNonQuery();
                    transaction.Commit();
                    Console.WriteLine("Удалось сгенерировать" + ID);
                    ID = ID + 1;
                    count = count + 1;
                    
                }
                catch 
                {
                    transaction.Rollback();
                    Console.WriteLine("Уже существует" + ID);
                    ID = ID + 1;
                    conn.Close();
                }
                conn.Close();
            }
            Console.WriteLine("Данные сгененированы" );
        }

        public static void updateBD()
        {
            DateTime DateTime = DateTime.Now;
            OracleConnection conn = DBUtils.GetDBConnection();
            conn.Open();
            string DateTime2 = DateTime.ToShortDateString();
            //int start = 0;
            //int finish = 10;
            
            //string sql = "select * FROM(SELECT ROWNUM rn, v.HASH, v.END_DATE,v.STATUS,v.WAITINGFOREXPORT,v.ID FROM CARDSTATUS2 v) WHERE rn >= "+start+ " AND rn <= " + finish + "";
            string sql = "SELECT HASH,STATUS,WAITINGFOREXPORT,ID from CARDSTATUS4 where END_DATE < to_date('" + DateTime2 + "', 'DD/MM/YY') and WAITINGFOREXPORT is NULL";
            OracleCommand cmd = new OracleCommand();
           
  
            cmd.Connection = conn;
            cmd.CommandText = sql;
            int works = 0;
            int threads = 0;

            try
            {
                using (DbDataReader reader = cmd.ExecuteReader())
                {
                   
                    while (reader.Read())
                    {
                        //string HASH = Convert.ToString(reader.GetValue(0));
                       // string STATUS = Convert.ToString(reader.GetValue(1));
                       // string WAITINGFOREXPORT = Convert.ToString(reader.GetValue(2));
                        string ID = Convert.ToString(reader.GetValue(3));
                        ThreadPool.GetAvailableThreads(out works, out threads);
                       // Console.WriteLine(works.ToString(), threads.ToString());

                        //Console.WriteLine(HASH+" "+STATUS+" " + WAITINGFOREXPORT +" "+ ID);
                            int threadCount = 0;
                            AutoResetEvent waitHandler = new AutoResetEvent(true);
                            ManualResetEvent finished = new ManualResetEvent(false);
                            Interlocked.Increment(ref threadCount);
                            ThreadPool.QueueUserWorkItem(delegate
                            {

                                OracleConnection conn1 = DBUtils.GetDBConnection();
                                conn1.Open();
                                OracleCommand cmd1 = conn1.CreateCommand();
                                OracleTransaction transaction;
                                transaction = conn.BeginTransaction(IsolationLevel.ReadCommitted);
                                cmd1.Transaction = transaction;
                                try
                                {
                                    string sql11 = "UPDATE CARDSTATUS4 SET STATUS = 'EXPIRED', WAITINGFOREXPORT='1' WHERE ID = '" + ID + "'";
                                    cmd1.Connection = conn1;
                                    cmd1.CommandText = sql11;
                                    cmd1.ExecuteNonQuery();
                                    conn1.Close();
                                    transaction.Commit();
                                    //Console.WriteLine("Завершился" + ID);
                                }

                                catch (Exception e)
                                {
                                    transaction.Rollback();
                                    Console.WriteLine("Ошибка" + e);
                                }

                                finally
                                {
                                    finished.Set();
                                    if (Interlocked.Decrement(ref threadCount) == 0)
                                     {
                                        finished.Set();
                                        conn1.Close();
                                    }
                                    // Console.WriteLine("Завершился" + ID);
                                }
                            });
                            finished.WaitOne();
                            //bool b = ManualResetEvent.WaitAll(new WaitHandle[] { waitHandler });
                       // if (b == false)
                       // {
                       //     Console.WriteLine("");
                     //   }
                    }
                    }

                Console.WriteLine(works.ToString(), threads.ToString());
                Console.WriteLine("STATUS и WAITINGFOREXPOR обновлен\n");
                conn.Close();
            }
            catch (Exception ex)
            {
                Console.WriteLine("## ERROR: " + ex.Message);
                Console.Read();
            }
        }
        private static void exporttocsv()
        {

            int worker = 5;
            int port = 5;
            bool v = ThreadPool.SetMaxThreads(worker, port);
            Console.WriteLine(v);

            OracleConnection conn = DBUtils.GetDBConnection();
            conn.Open();

            try
            {
                Console.WriteLine("Переводим данные в CSV");
                string sql = "SELECT * FROM CARDSTATUS4 WHERE WAITINGFOREXPORT=1";
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
                        string ID = Convert.ToString(reader.GetValue(4));
                        string WAITINGFOREXPORT = Convert.ToString(reader.GetValue(3));
                        //Console.WriteLine( ID + " " + HASH + " " + END_DATE + " " + STATUS + " ");
                        using (StreamWriter sw = new StreamWriter(@"C:\Downloads\1.csv", true, Encoding.Default))
                        {
                            sw.WriteLine(ID + " "+ HASH + " " + END_DATE + " " + WAITINGFOREXPORT + " " +  STATUS);

                            int threadCount = 0;
                            ManualResetEvent finished = new ManualResetEvent(false);
                            Interlocked.Increment(ref threadCount);
                            ThreadPool.QueueUserWorkItem(delegate
                            {


                                OracleConnection conn3 = DBUtils.GetDBConnection();
                                conn3.Open();
                                OracleCommand cmd1 = conn3.CreateCommand();
                                OracleTransaction transaction;
                                transaction = conn3.BeginTransaction(IsolationLevel.ReadCommitted);
                                cmd1.Transaction = transaction;

                                try
                                {
                                    cmd1.Connection = conn3;
                                    cmd1.CommandText = "UPDATE CARDSTATUS4 SET WAITINGFOREXPORT='' WHERE ID = '" + ID + "'";
                                    cmd1.ExecuteNonQuery();
                                    transaction.Commit();
                                    conn3.Close();
                                    Console.WriteLine("Обновление произошло" + ID);

                                }

                                catch (Exception e)
                                {
                                    transaction.Rollback();
                                    Console.WriteLine("Ошибка" + e);
                                }

                                finally
                                {
                                    if (Interlocked.Decrement(ref threadCount) == 0)
                                    {
                                        finished.Set();
                                        conn3.Close();
                                    }
                                }
                            });
                            finished.WaitOne();
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
    }
}
