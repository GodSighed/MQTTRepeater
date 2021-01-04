using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;

namespace MQTTRepeater.DBUtility
{
    public static class PubConstant
    {
        public static string GetConnectionString(string timeYear)
        {
            string _connectionString = ConfigurationManager.AppSettings[string.Format("ConnectionString{0}", timeYear)];
            string ConStringEncrypt = ConfigurationManager.AppSettings["ConStringEncrypt"];
            return _connectionString;
        }
    }
}
