using System;
using System.Collections.Generic;
using System.Text;

namespace MQTTRepeater.Model
{
    public class MQTTData
    {
        public string repeaterID = "123";
        public List<ParamData> paramDatas = new List<ParamData>();
        public List<Log> logs = new List<Log>();
    }
}
