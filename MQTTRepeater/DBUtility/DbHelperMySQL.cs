using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace MQTTRepeater.DBUtility
{
    public class DbHelperMySQL
    {
        string _connectionString;
        MySqlConnection _connection;
        public DbHelperMySQL(string connectionSuffix)
        {
            _connectionString = PubConstant.GetConnectionString(connectionSuffix);
            _connection = new MySqlConnection(_connectionString);
            _connection.Open();
        }
        public DataSet Quene(string strSql)
        {
            DataSet ds = new DataSet();
            try
            {
                if (_connection.State == ConnectionState.Open)
                {
                    MySqlDataAdapter command = new MySqlDataAdapter(strSql, _connection);
                    command.Fill(ds, "dsReturn");
                }
                else if (_connection.State == ConnectionState.Closed)
                {
                    _connection.Open();
                    MySqlDataAdapter command = new MySqlDataAdapter(strSql, _connection);
                    command.Fill(ds, "dsReturn");
                }
            }
            catch (Exception ex)
            {
                ds = null;
            }
            return ds;
        }
        /// <summary>
        /// 执行修改语句，返回修改结果
        /// </summary>
        /// <param name="strSql">sql语句</param>
        /// <param name="timeYear">操作年份</param>
        /// <returns></returns>
        public string Updata(string strSql)
        {
            string strReturn = "-1";
            try
            {
                if (_connection.State == ConnectionState.Open)
                {
                    MySqlCommand command = new MySqlCommand(strSql, _connection);
                    strReturn = command.ExecuteNonQuery().ToString();
                }
                else if (_connection.State == ConnectionState.Closed)
                {
                    _connection.Open();
                    MySqlCommand command = new MySqlCommand(strSql, _connection);
                    strReturn = command.ExecuteNonQuery().ToString();
                }
            }
            catch (Exception ex)
            {
                strReturn = ex.Message;
            }
            return strReturn;
        }
    }
}
