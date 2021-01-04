using MQTTnet;
using MQTTnet.Client;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Xml;

namespace MQTTRepeater
{
    class Program
    {
        private static int _heartBeat = 0;//心跳计数器
        private static Model.MyMqtt _myMqtt;
        private static IMqttClient _client;
        private static DBUtility.DbHelperMySQL _dbHelperMySQL;
        static void Main(string[] args)
        {
            PurVar.PurStatic.WriteToStatus("2s后开始");
            Thread.Sleep(2000);
            try
            {
                PurVar.PurStatic.WriteToStatus(System.AppDomain.CurrentDomain.BaseDirectory);
                _dbHelperMySQL = new DBUtility.DbHelperMySQL("Local");
                string strSql = "select * from system_infomation";
                DataSet dsSystemInfomation = _dbHelperMySQL.Quene(strSql);
                if (dsSystemInfomation != null)
                {
                    if (dsSystemInfomation.Tables.Count == 1)
                    {
                        if (dsSystemInfomation.Tables[0].Rows.Count == 1)
                        {
                            PurVar.Repeater.repeaterId = Convert.ToInt32(dsSystemInfomation.Tables[0].Rows[0]["repeater_id"]);
                            PurVar.Repeater.projectCode = Convert.ToInt32(dsSystemInfomation.Tables[0].Rows[0]["projectcode"]);
                            PurVar.Repeater.repeaterType = dsSystemInfomation.Tables[0].Rows[0]["repeater_type"].ToString();
                            PurVar.Repeater.firmwareVersion = dsSystemInfomation.Tables[0].Rows[0]["firmware_version"].ToString();
                        }
                        else { return; }
                    }
                    else { return; }
                }
                else { return; }
                XmlDocument xmlDoc = new XmlDocument();
                xmlDoc.Load(System.AppDomain.CurrentDomain.BaseDirectory+"BasicConfig.xml");
                XmlNode xnMQTTUrl = xmlDoc.SelectSingleNode("/root/MQTT/url");
                XmlNode xnMQTTPort = xmlDoc.SelectSingleNode("/root/MQTT/port");
                _myMqtt = new Model.MyMqtt()
                {
                    AllTopics = InitTopics(),
                    SelectedTopics = new List<TopicFilter>(),
                    ServerUri = xnMQTTUrl.InnerText,
                    UserName = "user",
                    Password = "123",
                    CurrentTopic = null,
                    ServerPort = Convert.ToInt32(xnMQTTPort.InnerText),
                    ClientID = PurVar.Repeater.repeaterId.ToString()
                };
                if (_myMqtt.ServerUri != null && _myMqtt.ServerPort > 0)
                {
                    InitClient(_myMqtt.ClientID, _myMqtt.ServerUri, _myMqtt.ServerPort);
                }
                else
                {
                    PurVar.PurStatic.WriteToStatus("提示,服务端地址或端口号不能为空！");
                    return;
                }
            }
            catch(Exception ex)
            { PurVar.PurStatic.WriteToStatus("初始化错误：" + ex.Message); }
            Thread tConnect = new Thread(WhilePublish);//上报数据方法
            tConnect.Start();
            Thread tHeartBeat = new Thread(HeartBeat);//心跳线程
            tHeartBeat.Start();
            Thread tReflashSub = new Thread(ReflashSub);//刷新订阅
            tReflashSub.Start();
        }

        /// <summary>
        /// 初始化系统信息
        /// </summary>
        private static void InitRepeaterSystem()
        {
            _dbHelperMySQL = new DBUtility.DbHelperMySQL("Local");
            string strSql = "select * from system_infomation";
            DataSet dsSystemInfomation = _dbHelperMySQL.Quene(strSql);
            if (dsSystemInfomation != null)
            {
                if (dsSystemInfomation.Tables.Count == 1)
                {
                    if (dsSystemInfomation.Tables[0].Rows.Count == 1)
                    {

                        PurVar.Repeater.repeaterId = Convert.ToInt32(dsSystemInfomation.Tables[0].Rows[0]["repeater_id"]);
                        PurVar.Repeater.projectCode = Convert.ToInt32(dsSystemInfomation.Tables[0].Rows[0]["projectcode"]);
                        PurVar.Repeater.repeaterType = dsSystemInfomation.Tables[0].Rows[0]["repeater_type"].ToString();
                        PurVar.Repeater.firmwareVersion = dsSystemInfomation.Tables[0].Rows[0]["firmware_version"].ToString();
                    }
                    else { PurVar.PurStatic.WriteToStatus("提示,初始化系统信息失败，5s后重试！"); Thread.Sleep(5000); InitRepeaterSystem(); }
                }
                else { PurVar.PurStatic.WriteToStatus("提示,初始化系统信息失败，5s后重试！"); Thread.Sleep(5000); InitRepeaterSystem(); }
            }
        }

        /// <summary>
        /// 刷新订阅的topic
        /// </summary>
        private static void ReflashSub()
        {
            while(true)
            {
                GetSub();
                Thread.Sleep(3600000);
            }
        }

        /// <summary>
        /// 获得订阅
        /// </summary>
        private static void GetSub()
        {
            if (_myMqtt.IsConnected == true)
            {
                try
                {
                    long startParamid = PurVar.PurStatic.GetParamID(PurVar.Repeater.projectCode, PurVar.Repeater.repeaterId, 0x2000);
                    long endParamid = PurVar.PurStatic.GetParamID(PurVar.Repeater.projectCode, PurVar.Repeater.repeaterId, 0x3FFF);
                    string strSql = string.Format("select paramid from param_temp where paramid>{0} and paramid<{1}", startParamid, endParamid);
                    DataSet dsParamid = _dbHelperMySQL.Quene(strSql);
                    if (dsParamid==null)
                    {
                        PurVar.PurStatic.WriteToStatus("等待数据库启动");
                        Thread.Sleep(5000);
                        GetSub();
                        return;
                    }

                    foreach (DataRow drParamid in dsParamid.Tables[0].Rows)
                    {
                        if (_myMqtt.IsConnected == true)
                        {
                            _client.SubscribeAsync(drParamid["paramid"].ToString());//订阅输入
                            PurVar.PurStatic.WriteToStatus("订阅" + drParamid["paramid"].ToString() + "成功");
                        }
                    }
                }
                catch (Exception ex) { PurVar.PurStatic.WriteToStatus("订阅错误" + ex.Message); }
            }
        }

        /// <summary>
        /// 心跳包
        /// </summary>
        private static void HeartBeat()
        {

            while (true)
            {
                try { PublishRepeater("hearbeat/" + _myMqtt.ClientID, DateTime.Now.ToString("yyyyMMddHHmmss")); }
                catch(Exception ex) { PurVar.PurStatic.WriteToStatus("心跳包发送错误：" + ex.Message); }
                Thread.Sleep(5000);
            }
        }

        /// <summary>
        /// 循环发送数据
        /// </summary>
        private static void WhilePublish()
        {
            DateTime tempTime = DateTime.Now;
            DateTime logTime = DateTime.Now;
            while(true)
            {
                try
                {
                    string strSql = "select * from param_temp";
                    string strSqlLog = string.Format("select * from log where logtime>'{0}'", logTime.ToString("yyyy/MM/dd HH:mm:ss"));
                    DataSet dataSet = _dbHelperMySQL.Quene(strSql);
                    DataSet dsLog = _dbHelperMySQL.Quene(strSqlLog);
                    Model.MQTTData mQTTData = new Model.MQTTData();
                    mQTTData.repeaterID = PurVar.Repeater.repeaterId.ToString();
                    //添加数据信息
                    if (dataSet != null)
                    {
                        if (dataSet.Tables.Count == 1)
                        {
                            foreach (DataRow dataRow in dataSet.Tables[0].Rows)
                            {
                                Model.ParamData paramData = new Model.ParamData();
                                paramData.currentValue = Convert.ToDouble(dataRow["currentvalue"]);
                                paramData.paramID = Convert.ToInt64(dataRow["paramid"]);
                                paramData.saveTime = Convert.ToDateTime(dataRow["savetime"]);
                                paramData.paramTemp = Convert.ToInt32(dataRow["param_temp"]);
                                mQTTData.paramDatas.Add(paramData);
                                if(tempTime<Convert.ToDateTime(dataRow["savetime"])) { tempTime = Convert.ToDateTime(dataRow["savetime"]); }
                            }
                        }
                    }
                    //添加日志信息
                    if (dsLog != null)
                    {
                        if (dsLog.Tables.Count == 1)
                        {
                            foreach(DataRow drLog in dsLog.Tables[0].Rows)
                            {
                                Model.Log log = new Model.Log();
                                log.content = drLog["content"].ToString();
                                log.logTime = Convert.ToDateTime(drLog["logtime"]);
                                log.logType = drLog["logtype"].ToString();
                                if(logTime< Convert.ToDateTime(drLog["logtime"]))
                                { logTime = Convert.ToDateTime(drLog["logtime"]); }
                                mQTTData.logs.Add(log);
                            }
                        }
                    }
                    string strJsonData = JsonConvert.SerializeObject(mQTTData);
                    
                    PublishRepeater("service/" + PurVar.Repeater.repeaterId.ToString() + "/data", strJsonData);
                }
                catch (Exception em)
                { PurVar.PurStatic.WriteToStatus(em.Message); }
                Thread.Sleep(5000);
            }
        }

        private static void SubscribeTest(List<TopicFilter> filters)
        {
            if (_client != null)
            {
                _client.SubscribeAsync("#");
                string tmp = "";
                foreach (var filter in filters)
                {
                    tmp += filter.Topic;
                    tmp += ",";
                }
                if (tmp.Length > 1)
                {
                    tmp = tmp.Substring(0, tmp.Length - 1);
                }
                PurVar.PurStatic.WriteToStatus("成功订阅主题：" + tmp);
            }
            else
            {
                PurVar.PurStatic.WriteToStatus("提示,请连接服务端后订阅主题！");
            }
        }
        /// <summary>
        /// publish数据
        /// </summary>
        private static void PublishRepeater(string topic, string message)
        {
            try
            {
                if (_myMqtt.IsConnected == true)
                {
                    MqttApplicationMessage msg = new MqttApplicationMessage
                    {
                        Topic = topic,
                        Payload = Encoding.UTF8.GetBytes(message),
                        QualityOfServiceLevel = MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce,
                        Retain = false
                    };
                    _client.PublishAsync(msg);
                    PurVar.PurStatic.WriteToStatus("成功发布主题为" + topic + "的消息！");
                }  
            }
            catch (Exception e)
            {
                PurVar.PurStatic.WriteToStatus(e.Message);
                _heartBeat++;
                if (_heartBeat > 5)
                {
                    _myMqtt.IsConnected = false;
                    InitClient(_myMqtt.ClientID, _myMqtt.ServerUri, _myMqtt.ServerPort);
                }
            }
        }
        /// <summary>
        /// 数据初始化
        /// </summary>
        /// <returns></returns>
        private static List<Model.MyTopic> InitTopics()
        {
            List<Model.MyTopic> topics = new List<Model.MyTopic>();
            topics.Add(new Model.MyTopic("jjk", "环境-温度"));
            topics.Add(new Model.MyTopic("/environ/hum", "环境-湿度"));
            //topics.Add(new TopicModel("/environ/pm25", "环境-PM2.5"));
            //topics.Add(new TopicModel("/environ/CO2", "环境-二氧化碳"));
            //topics.Add(new TopicModel("/energy/electric", "能耗-电"));
            //topics.Add(new TopicModel("/energy/water", "环境-水"));
            //topics.Add(new TopicModel("/energy/gas", "环境-电"));
            topics.Add(new Model.MyTopic("/data/alarm", "数据-报警"));
            topics.Add(new Model.MyTopic("/data/message", "数据-消息"));
            topics.Add(new Model.MyTopic("/data/notify", "数据-通知"));
            return topics;
        }
        /// <summary>
        /// 数据模型转换
        /// </summary>
        /// <param name="topics"></param>
        /// <returns></returns>
        private static List<TopicFilter> ConvertTopics(List<Model.MyTopic> topics)
        {
            //MQTTnet.TopicFilter
            List<TopicFilter> filters = new List<TopicFilter>();
            foreach (Model.MyTopic model in topics)
            {
                TopicFilter filter = new TopicFilter(model.Topic, MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce);
                filters.Add(filter);

            }
            return filters;
        }
        /// <summary>
        /// 初始化
        /// </summary>
        /// <param name="id"></param>
        /// <param name="url"></param>
        /// <param name="port"></param>
        private static void InitClient(string id, string url = "127.0.0.1", int port = 1883)
        {
            var options = new MqttClientOptions()
            {
                ClientId = id
            };
            options.ChannelOptions = new MqttClientTcpOptions()
            {
                Server = url,
                Port = port
            };
            options.Credentials = new MqttClientCredentials()
            {
                Username = _myMqtt.UserName,
                Password = _myMqtt.Password
            };
            options.CleanSession = true;
            options.KeepAlivePeriod = TimeSpan.FromSeconds(100);
            options.KeepAliveSendInterval = TimeSpan.FromSeconds(10000);
            if (_client != null)
            {
                if (_myMqtt.IsConnected == true)
                { _client.DisconnectAsync(); }
                _client = null;
            }
            _client = new MQTTnet.MqttFactory().CreateMqttClient();
            _client.ApplicationMessageReceived += _client_ApplicationMessageReceived;
            _client.Connected += _client_Connected;
            _client.Disconnected += _client_Disconnected;
            _client.ConnectAsync(options);
        }
        /// <summary>
        /// 客户端与服务端断开连接
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void _client_Disconnected(object sender, MqttClientDisconnectedEventArgs e)
        {
            _myMqtt.IsConnected = false;
            _myMqtt.IsDisConnected = true;
            PurVar.PurStatic.WriteToStatus("与服务端断开连接！5s后重连");
            Thread.Sleep(5000);
            InitClient(_myMqtt.ClientID, _myMqtt.ServerUri, _myMqtt.ServerPort);
        }

        /// <summary>
        /// 客户端与服务端建立连接
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>

        private static void _client_Connected(object sender, MqttClientConnectedEventArgs e)
        {
            _myMqtt.IsConnected = true;
            _myMqtt.IsDisConnected = false;
            PurVar.PurStatic.WriteToStatus("与服务端建立连接");
            GetSub();
            //_client.SubscribeAsync(PurVar.Repeater.repeaterId.ToString() + "/#");//订阅本机id
            //PurVar.PurStatic.WriteToStatus("订阅" + PurVar.Repeater.repeaterId.ToString() + "/#" + "成功");
        }

        /// <summary>
        /// 客户端收到消息
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private static void _client_ApplicationMessageReceived(object sender, MQTTnet.MqttApplicationMessageReceivedEventArgs e)
        {
            PurVar.PurStatic.WriteToStatus("收到来自客户端" + e.ClientId + "，主题为" + e.ApplicationMessage.Topic + "的消息：" + Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
            string strSql = string.Format("update param_temp set needset=1,setvalue={1} where paramid={0}", e.ApplicationMessage.Topic, Encoding.UTF8.GetString(e.ApplicationMessage.Payload));
            string message = _dbHelperMySQL.Updata(strSql);
            PublishRepeater("service/" + PurVar.Repeater.repeaterId.ToString() + "/writeMessage", message);
        }
    }
}
