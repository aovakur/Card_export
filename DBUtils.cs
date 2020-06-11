using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Oracle.DataAccess.Client;

namespace ConsoleApp13
{
    class DBUtils
    {

        public static OracleConnection GetDBConnection()
        {
            string host = "127.0.0.1";
            int port = 1521;
            string sid = "XE";
            string user = "Andrey";
            string password = "aA123456";

            return DBOracleUtils.GetDBConnection(host, port, sid, user, password);
        }
    }

}