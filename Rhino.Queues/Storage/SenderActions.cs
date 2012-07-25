using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;

using Microsoft.Isam.Esent.Interop;
using Rhino.Queues.Model;
using Rhino.Queues.Protocol;

namespace Rhino.Queues.Storage
{
    /// <summary>
    /// A logger.
    /// </summary>
    public interface ILog
    {
        /// <summary>
        /// Logs the message as info.
        /// </summary>
        /// <param name="message">The message.</param>
        void Info(string message);

        /// <summary>
        /// Logs the message as a warning.
        /// </summary>
        /// <param name="message">The message.</param>
        void Warn(string message);

        /// <summary>
        /// Logs the message as a warning with the related exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="exception">The exception.</param>
        void Warn(string message, Exception exception);

        /// <summary>
        /// Logs the message as debug information.
        /// </summary>
        /// <param name="message">The message.</param>
        void Debug(string message);

        /// <summary>
        /// Logs the message as debug information with the related exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="exception">The exception.</param>
        void Debug(string message, Exception exception);

        /// <summary>
        /// Logs the message as an error.
        /// </summary>
        /// <param name="message">The message.</param>
        void Error(string message);

        /// <summary>
        /// Logs the exception.
        /// </summary>
        /// <param name="exception">The exception.</param>
        void Error(Exception exception);

        /// <summary>
        /// Logs the specified message along with the related exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="exception">The exception.</param>
        void Error(string message, Exception exception);

        /// <summary>
        /// Logs the message.
        /// </summary>
        /// <param name="message">The message.</param>
        void Fatal(string message);

        /// <summary>
        /// Logs the specified message along with the related exception.
        /// </summary>
        /// <param name="message">The message.</param>
        /// <param name="exception">The exception.</param>
        void Fatal(string message, Exception exception);
    }

    /// <summary>
    /// Used to manage logging.
    /// </summary>
    public static class LogManager
    {
        static readonly ILog NullLogSingleton = new NullLog();
        static Func<Type, ILog> _logLocator = type => NullLogSingleton;

        /// <summary>
        /// Initializes the system with the specified log creator.
        /// </summary>
        /// <param name="logLocator">The log locator.</param>
        public static void Initialize(Func<Type, ILog> logLocator)
        {
            _logLocator = logLocator;
        }

        /// <summary>
        /// Creates a log.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public static ILog GetLogger(Type type)
        {
            return _logLocator(type);
        }

        private class NullLog : ILog
        {
            public void Info(string message) { }
            public void Warn(string message) { }
            public void Warn(string message, Exception exception) { }
            public void Debug(string message) { }
            public void Debug(string message, Exception exception) { }
            public void Error(string message) { }
            public void Error(Exception exception) { }
            public void Error(string message, Exception exception) { }
            public void Fatal(string message) { }
            public void Fatal(string message, Exception exception) { }
        }
    }

    public static class LogExtensions
    {
        /// <summary>
        /// Logs the message as info.
        /// </summary>
        /// <param name="log">The log.</param>
        /// <param name="format">The message.</param>
        /// <param name="args">The args.</param>
        public static void InfoFormat(this ILog log, string format, params object[] args)
        {
            log.Info(string.Format(format, args));
        }

        /// <summary>
        /// Logs the message as info.
        /// </summary>
        /// <param name="log">The log.</param>
        /// <param name="format">The message.</param>
        /// <param name="args">The args.</param>
        /// <param name="e">The exception</param>
        public static void DebugFormat(this ILog log, Exception e, string format, params object[] args)
        {
            log.Debug(string.Format(format, args), e);
        }

        /// <summary>
        /// Logs the message as info.
        /// </summary>
        /// <param name="log">The log.</param>
        /// <param name="format">The message.</param>
        /// <param name="args">The args.</param>
        public static void DebugFormat(this ILog log, string format, params object[] args)
        {
            log.Debug(string.Format(format, args));
        }

        /// <summary>
        /// Logs the message as a warning.
        /// </summary>
        /// <param name="log">The log.</param>
        /// <param name="format">The message.</param>
        /// <param name="args">The args.</param>
        public static void WarnFormat(this ILog log, string format, params object[] args)
        {
            log.Info(string.Format(format, args));
        }

        /// <summary>
        /// Logs the message as an error.
        /// </summary>
        /// <param name="log">The log.</param>
        /// <param name="format">The message.</param>
        /// <param name="args">The args.</param>
        public static void ErrorFormat(this ILog log, string format, params object[] args)
        {
            log.Error(string.Format(format, args));
        }
    }

    public class SenderActions : AbstractActions
    {
        private readonly ILog logger = LogManager.GetLogger(typeof(SenderActions));

		public SenderActions(JET_INSTANCE instance, ColumnsInformation columnsInformation, string database, Guid instanceId)
            : base(instance, columnsInformation, database, instanceId)
        {
        }

        public IList<PersistentMessage> GetMessagesToSendAndMarkThemAsInFlight(int maxNumberOfMessage, int maxSizeOfMessagesInTotal, out Endpoint endPoint)
        {
            Api.MoveBeforeFirst(session, outgoing);

            endPoint = null;
        	string queue = null;
            var messages = new List<PersistentMessage>();

            while (Api.TryMoveNext(session, outgoing))
            {

				var msgId = new Guid(Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]));
                var value = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"]).Value;
                var timeAsDate = Api.RetrieveColumnAsDouble(session, outgoing, ColumnsInformation.OutgoingColumns["time_to_send"]).Value;
                var time = DateTime.FromOADate(timeAsDate);

                logger.DebugFormat("Scanning message {0} with status {1} to be sent at {2}", msgId, value, time);
                if (value != OutgoingMessageStatus.Ready)
                    continue;

                if (time > DateTime.Now)
                    continue;

                var rowEndpoint = new Endpoint(
                    Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["address"]),
                    Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["port"]).Value
                    );

                if (endPoint == null)
                    endPoint = rowEndpoint;

                if (endPoint.Equals(rowEndpoint) == false)
                    continue;

				var rowQueue = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["queue"], Encoding.Unicode);

				if (queue == null) 
					queue = rowQueue;

				if(queue != rowQueue)
					continue;

				logger.DebugFormat("Adding message {0} to returned messages", msgId);
               
                var bookmark = new MessageBookmark();
                Api.JetGetBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size, out bookmark.Size);
                var headerAsQueryString = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["headers"],Encoding.Unicode);
            	messages.Add(new PersistentMessage
                {
                    Id = new MessageId
                    {
                        SourceInstanceId = instanceId,
                        MessageIdentifier = msgId
                    },
                    Headers = HttpUtility.ParseQueryString(headerAsQueryString),
                    Queue = rowQueue,
                    SubQueue = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["subqueue"], Encoding.Unicode),
                    SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, outgoing, ColumnsInformation.OutgoingColumns["sent_at"]).Value),
                    Data = Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["data"]),
                    Bookmark = bookmark
                });

                using (var update = new Update(session, outgoing, JET_prep.Replace))
                {
                    Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"],
                                  (int)OutgoingMessageStatus.InFlight);
                    update.Save();
                }

                logger.DebugFormat("Marking output message {0} as InFlight", msgId);

                if (maxNumberOfMessage < messages.Count)
                    break;
                if (maxSizeOfMessagesInTotal < messages.Sum(x => x.Data.Length))
                    break;
            }
            return messages;
        }

        public void MarkOutgoingMessageAsFailedTransmission(MessageBookmark bookmark, bool queueDoesNotExistsInDestination)
        {
            Api.JetGotoBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size);
            var numOfRetries = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["number_of_retries"]).Value;
            var msgId = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]).Value;

            if (numOfRetries < 100 && queueDoesNotExistsInDestination == false)
            {
                using (var update = new Update(session, outgoing, JET_prep.Replace))
                {
                    var timeToSend = DateTime.Now.AddSeconds(numOfRetries * numOfRetries);


                    Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"], (int)OutgoingMessageStatus.Ready);
                    Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["time_to_send"],
                                  timeToSend.ToOADate());
                    Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["number_of_retries"],
                                  numOfRetries + 1);

                    logger.DebugFormat("Marking outgoing message {0} as failed with retries: {1}",
                                       msgId, numOfRetries);

                    update.Save();
                }
            }
            else
            {
                using (var update = new Update(session, outgoingHistory, JET_prep.Insert))
                {
                    foreach (var column in ColumnsInformation.OutgoingColumns.Keys)
                    {
                        Api.SetColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns[column],
                            Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns[column])
                            );
                    }
					Api.SetColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns["send_status"],
                        (int)OutgoingMessageStatus.Failed);

                    logger.DebugFormat("Marking outgoing message {0} as permenantly failed after {1} retries",
                                       msgId, numOfRetries);

                    update.Save();
                }
                Api.JetDelete(session, outgoing);
            }
        }

        public MessageBookmark MarkOutgoingMessageAsSuccessfullySent(MessageBookmark bookmark)
        {
            Api.JetGotoBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size);
            var newBookmark = new MessageBookmark();
            using (var update = new Update(session, outgoingHistory, JET_prep.Insert))
            {
                foreach (var column in ColumnsInformation.OutgoingColumns.Keys)
                {
                	var bytes = Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns[column]);
					Api.SetColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns[column], bytes);
                }
				Api.SetColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns["send_status"],
                              (int)OutgoingMessageStatus.Sent);

                update.Save(newBookmark.Bookmark, newBookmark.Size, out newBookmark.Size);
            }
            var msgId = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]).Value;
            Api.JetDelete(session, outgoing);
            logger.DebugFormat("Successfully sent output message {0}", msgId);
            return newBookmark;
        }

    	public bool HasMessagesToSend()
		{
			Api.MoveBeforeFirst(session, outgoing);
			return Api.TryMoveNext(session, outgoing);
		}

        public IEnumerable<PersistentMessageToSend> GetMessagesToSend()
        {
            Api.MoveBeforeFirst(session, outgoing);

            while (Api.TryMoveNext(session, outgoing))
            {
                var address = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["address"]);
                var port = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["port"]).Value;

                var bookmark = new MessageBookmark();
                Api.JetGetBookmark(session, outgoing, bookmark.Bookmark, bookmark.Size, out bookmark.Size);

                yield return new PersistentMessageToSend
                {
                    Id = new MessageId
                    {
                        SourceInstanceId = instanceId,
						MessageIdentifier = new Guid(Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]))
                    },
                    OutgoingStatus = (OutgoingMessageStatus)Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"]).Value,
                    Endpoint = new Endpoint(address, port),
                    Queue = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["queue"], Encoding.Unicode),
                    SubQueue = Api.RetrieveColumnAsString(session, outgoing, ColumnsInformation.OutgoingColumns["subqueue"], Encoding.Unicode),
                    SentAt = DateTime.FromOADate(Api.RetrieveColumnAsDouble(session, outgoing, ColumnsInformation.OutgoingColumns["sent_at"]).Value),
                    Data = Api.RetrieveColumn(session, outgoing, ColumnsInformation.OutgoingColumns["data"]),
                    Bookmark = bookmark
                };
            }
        }

        public void RevertBackToSend(MessageBookmark[] bookmarks)
        {
            foreach (var bookmark in bookmarks)
            {
                Api.JetGotoBookmark(session, outgoingHistory, bookmark.Bookmark, bookmark.Size);
                var msgId = Api.RetrieveColumnAsInt32(session, outgoing, ColumnsInformation.OutgoingColumns["msg_id"]).Value;

                using(var update = new  Update(session, outgoing, JET_prep.Insert))
                {
                    foreach (var column in ColumnsInformation.OutgoingColumns.Keys)
                    {
                        Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns[column],
							Api.RetrieveColumn(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns[column])
                            );
                    }
					Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["send_status"],
						(int)OutgoingMessageStatus.Ready);
					Api.SetColumn(session, outgoing, ColumnsInformation.OutgoingColumns["number_of_retries"],
						Api.RetrieveColumnAsInt32(session, outgoingHistory, ColumnsInformation.OutgoingHistoryColumns["number_of_retries"]).Value + 1
						   );

                    logger.DebugFormat("Reverting output message {0} back to Ready mode", msgId);

                    update.Save();
                }
            }
        }
    }
}