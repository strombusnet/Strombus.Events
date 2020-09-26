using Strombus.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Text;

namespace Strombus.Events
{
    public class EventClient
    {
        private static RedisClient _redisClient;

        private const string REDIS_PREFIX_SERVICE = "service";
        private const string REDIS_PREFIX_SEPARATOR = ":";
        //
        private const string REDIS_ASTERISK = "*";
        private const string REDIS_SLASH = "/";
        private const char REDIS_SLASH_AS_CHAR = '/';
        //
        private const string REDIS_SUFFIX_INCOMING_NOTIFICATIONS = "incoming-notifications";
        private const string REDIS_SUFFIX_SEPARATOR = "#";

        private const string EVENT_SERVICE_NAME = "event";

        public static async Task RaiseEventAsync(Notification notification, PriorityLevel priority, string resourcePath)
        {
            // ensure that we have a connection to the Redis server
            // NOTE: we could do this in our constructor, but we have intentionally avoided putting any code there which could block or throw an exception.  Instead we lazily load the redis client here.
            if (_redisClient == null)
            {
                // NOTE: this call will attempt to create the connection if it does not already exists (and will "block" in an async-friendly fashion)
                _redisClient = await Singletons.GetRedisClientAsync();
            }

            // make sure that the event priority is within the valid range
            if ((int)priority < 0 || (int)priority > 7)
            {
                throw new ArgumentOutOfRangeException("Priority must be in the range {0...7}", nameof(priority));
            }
            // make sure that our event has a name
            if (string.IsNullOrEmpty(notification.event_name))
            {
                throw new ArgumentException("Every event must have an event name", nameof(notification));
            }

            // set our event's timestamp
            // NOTE: the timestamp could alternatively be set by Redis (in an atomic transaction, using TIME followed by RPUSH, etc.)
            long createdTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            notification.notification_time = createdTimestamp;
            // make sure that the event has a data collection (even if it's empty)
            if (notification.data == null)
            {
                notification.data = new Dictionary<string, string>();
            }

            // encode event as JSON
            string jsonEncodedEvent = JsonConvert.SerializeObject(notification, Formatting.None, new JsonSerializerSettings() { NullValueHandling = NullValueHandling.Ignore });
            byte[] createdTimestampAsBytes = System.BitConverter.GetBytes(createdTimestamp * 1000);
            byte[] resourcePathAndEventAsBytes = Encoding.UTF8.GetBytes(resourcePath + REDIS_SUFFIX_SEPARATOR + notification.event_name);
            byte[] jsonEncodedEventAsBytes = System.Text.Encoding.UTF8.GetBytes(jsonEncodedEvent);
            byte[] valueAsBytes = new byte[createdTimestampAsBytes.Length + resourcePathAndEventAsBytes.Length + 1 + jsonEncodedEventAsBytes.Length];
            int valueAsBytesPos = 0;
            Array.Copy(createdTimestampAsBytes, 0, valueAsBytes, valueAsBytesPos, createdTimestampAsBytes.Length);
            valueAsBytesPos += createdTimestampAsBytes.Length;
            Array.Copy(resourcePathAndEventAsBytes, 0, valueAsBytes, valueAsBytesPos, resourcePathAndEventAsBytes.Length);
            valueAsBytesPos += resourcePathAndEventAsBytes.Length;
            valueAsBytes[valueAsBytesPos] = 0x00; // we insert a 0x00 byte between the resource path and the event json as a marker (NOTE: 0x00 should never naturally occur in the middle of a UTF8 string)
            valueAsBytesPos++;
            Array.Copy(jsonEncodedEventAsBytes, 0, valueAsBytes, valueAsBytesPos, jsonEncodedEventAsBytes.Length);

            await _redisClient.ListPushRightAsync<string, byte[]>(REDIS_PREFIX_SERVICE + REDIS_PREFIX_SEPARATOR + REDIS_ASTERISK + REDIS_SLASH + EVENT_SERVICE_NAME + REDIS_SUFFIX_SEPARATOR + REDIS_SUFFIX_INCOMING_NOTIFICATIONS + ((int)priority).ToString(), new byte[][] { valueAsBytes });
        }
    }
}
