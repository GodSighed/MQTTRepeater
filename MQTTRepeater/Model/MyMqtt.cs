using MQTTnet;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace MQTTRepeater.Model
{
    public class MyMqtt: INotifyPropertyChanged
    {
        private List<MyTopic> _allTopics;

        public List<MyTopic> AllTopics
        {
            get { return _allTopics; }
            set
            {
                if (_allTopics != value)
                {
                    _allTopics = value;
                    OnPropertyChanged("AllTopics");
                }
            }
        }

        private List<TopicFilter> _selectedTopics;

        public List<TopicFilter> SelectedTopics
        {
            get { return _selectedTopics; }
            set
            {
                if (_selectedTopics != value)
                {
                    _selectedTopics = value;
                    OnPropertyChanged("SelectedTopics");
                }
            }
        }

        private string _serverUri;

        public string ServerUri
        {
            get { return _serverUri; }
            set
            {
                if (_serverUri != value)
                {
                    _serverUri = value;
                    OnPropertyChanged("ServerUri");
                }
            }
        }

        private int _serverPort;

        public int ServerPort
        {
            get { return _serverPort; }
            set
            {
                if (_serverPort != value)
                {
                    _serverPort = value;
                    OnPropertyChanged("ServerPort");
                }
            }
        }


        private string _clientId;

        public string ClientID
        {
            get { return _clientId; }
            set
            {
                if (_clientId != value)
                {
                    _clientId = value;
                    OnPropertyChanged("ClientID");
                }
            }
        }

        private TopicFilter _currentTopic;

        public TopicFilter CurrentTopic
        {
            get { return _currentTopic; }
            set
            {
                if (_currentTopic != value)
                {
                    _currentTopic = value;
                    OnPropertyChanged("CurrentTopic");
                }
            }
        }

        private bool? _isConnected = false;

        public bool? IsConnected
        {
            get { return _isConnected; }
            set
            {
                if (_isConnected != value)
                {
                    _isConnected = value;
                    OnPropertyChanged("IsConnected");
                }
            }
        }

        private bool _isDisConnected = true;

        public bool IsDisConnected
        {
            get { return _isDisConnected; }
            set
            {
                if (_isDisConnected != value)
                {
                    _isDisConnected = value;
                    this.OnPropertyChanged("IsDisConnected");
                }
            }
        }

        private string _userName = "admin";

        public string UserName
        {
            get { return _userName; }
            set
            {
                if (_userName != value)
                {
                    _userName = value;
                    this.OnPropertyChanged("UserName");
                }

            }
        }

        private string _password = "password";

        public string Password
        {
            get { return _password; }
            set
            {
                if (_password != value)
                {
                    _password = value;
                    this.OnPropertyChanged("Password");
                }

            }
        }



        public event PropertyChangedEventHandler PropertyChanged;
        protected virtual void OnPropertyChanged(string propertyName = null)
        {
            var handler = PropertyChanged;
            if (handler != null) handler(this, new PropertyChangedEventArgs(propertyName));
        }
    }
}
