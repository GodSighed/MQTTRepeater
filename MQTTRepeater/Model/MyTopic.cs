using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace MQTTRepeater.Model
{
    public class MyTopic: INotifyPropertyChanged
    {
        public MyTopic()
        {

        }
        public MyTopic(string topic, string describe)
        {
            _isSelected = false;
            _topic = topic;
            _describe = describe;
        }
        private bool? _isSelected;

        public bool? IsSelected
        {
            get { return _isSelected; }
            set
            {
                if (_isSelected != value)
                {
                    _isSelected = value;
                    OnPropertyChanged("IsSelected");
                }
            }
        }

        private string _topic;


        public string Topic
        {
            get { return _topic; }
            set
            {
                if (_topic != value)
                {
                    _topic = value;
                    OnPropertyChanged("Topic");
                }
            }
        }

        private string _describe;

        public string Describe
        {
            get { return _describe; }
            set
            {
                if (_describe != value)
                {
                    _describe = value;
                    OnPropertyChanged("Describe");
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
