using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Strombus.Events
{
    public struct Notification
    {
        // unique id of this event notification (event occurance)
        public string uid;
        // event name
        public string event_name;
        // notification timestamp (milliseconds since unix epoch)
        public long notification_time;
        // event data
        public Dictionary<string, string> data;
    }
}
