using System;
using System.Collections.Generic;
using System.Text;

namespace MQTTRepeater.PurVar
{
    public static class PurStatic
    {
        public static int count = 0;
        /// <summary>
        /// 状态输出
        /// </summary>
        /// <param name="message"></param>
        public static void WriteToStatus(string message)
        {
            string strTime = "[" + System.DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss") + "] ";
            Console.WriteLine(strTime + message + "\r");
            count++;
            if (count > 100)
            {
                Console.Clear();
                count = 0;
            }
        }
        /// <summary>
        /// 生成paramid
        /// </summary>
        /// <param name="projectID">项目id</param>
        /// <param name="repeaterID">repeaterid</param>
        /// <param name="pointID">点位id</param>
        /// <returns></returns>
        public static long GetParamID(long projectID,long repeaterID,long pointID)
        {
            return (long)(Math.Pow(16,6)*projectID+ Math.Pow(16, 4) *repeaterID+pointID);
        }
    }
}
