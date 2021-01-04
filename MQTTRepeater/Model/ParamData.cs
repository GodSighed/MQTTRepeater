using System;
using System.Collections.Generic;
using System.Text;

namespace MQTTRepeater.Model
{
    public class ParamData
    {
        public long paramID = 0;
        public double currentValue = 0;
        public DateTime saveTime;
        public int paramTemp = 0;
    }
}
