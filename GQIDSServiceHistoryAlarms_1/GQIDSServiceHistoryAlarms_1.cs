namespace GQIDSServiceAlarms_1
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Threading.Tasks;
	using Skyline.DataMiner.Analytics.GenericInterface;
	using Skyline.DataMiner.Net.Filters;
	using Skyline.DataMiner.Net.Messages;

	[GQIMetaData(Name = "Service history alarms")]
	public class ServiceAlarms : IGQIDataSource, IGQIOnInit, IGQIOnPrepareFetch, IGQIInputArguments
	{
		private static readonly GQIStringArgument Service = new GQIStringArgument("Service") { IsRequired = true };
		private static readonly GQIDateTimeArgument StartTime = new GQIDateTimeArgument("Start Time") { IsRequired = true };
		private static readonly GQIDateTimeArgument EndTime = new GQIDateTimeArgument("End Time") { IsRequired = true };

		private static readonly GQIStringColumn IDColumn = new GQIStringColumn("ID");
		private static readonly GQIStringColumn ElementColumn = new GQIStringColumn("Element");
		private static readonly GQIStringColumn ParameterColumn = new GQIStringColumn("Parameter");
		private static readonly GQIStringColumn ValueColumn = new GQIStringColumn("Value");
		private static readonly GQIDateTimeColumn TimeColumn = new GQIDateTimeColumn("Time");
		private static readonly GQIStringColumn SeverityColumn = new GQIStringColumn("Severity");
		private static readonly GQIStringColumn OwnerColumn = new GQIStringColumn("Owner");

		private GQIDMS _dms;
		private Task<List<AlarmEventMessage>> _alarms;
		private string _service;
		private DateTime _startTime;
		private DateTime _endTime;

		public OnInitOutputArgs OnInit(OnInitInputArgs args)
		{
			_dms = args.DMS;
			return new OnInitOutputArgs();
		}

		public GQIArgument[] GetInputArguments()
		{
			return new GQIArgument[]
			{
				Service,
				StartTime,
				EndTime,
			};
		}

		public OnArgumentsProcessedOutputArgs OnArgumentsProcessed(OnArgumentsProcessedInputArgs args)
		{
			args.TryGetArgumentValue(Service, out _service);
			_startTime = args.GetArgumentValue(StartTime);
			_endTime = args.GetArgumentValue(EndTime);
			return new OnArgumentsProcessedOutputArgs();
		}

		public OnPrepareFetchOutputArgs OnPrepareFetch(OnPrepareFetchInputArgs args)
		{
			if (string.IsNullOrWhiteSpace(_service))
				return new OnPrepareFetchOutputArgs();

			_alarms = Task.Factory.StartNew(() =>
			{
				var service = _service.Replace(":", "_");
				var alarmFilterByProperty = new AlarmFilterItemString(AlarmFilterField.ServiceName, AlarmFilterCompareType.WildcardEquality, new string[] { "*" + service + "*" });
				var filter = new AlarmFilter(alarmFilterByProperty);
				var historyAlarmsMessage = new GetAlarmDetailsFromDbMessage
				{
					StartTime = _startTime,
					EndTime = _endTime,
					AlarmTable = true,
					Filter = filter,
				};

				var alarmsResponse = _dms.SendMessages(historyAlarmsMessage);
				if (alarmsResponse != null && alarmsResponse.Any())
				{
					return alarmsResponse.Select(alarm => alarm as AlarmEventMessage).ToList();
				}

				return null;
			});
			return new OnPrepareFetchOutputArgs();
		}

		public GQIColumn[] GetColumns()
		{
			return new GQIColumn[]
			{
				IDColumn,
				ElementColumn,
				ParameterColumn,
				ValueColumn,
				TimeColumn,
				SeverityColumn,
				OwnerColumn,
			};
		}

		public GQIPage GetNextPage(GetNextPageInputArgs args)
		{
			if (_alarms == null)
				return new GQIPage(new GQIRow[0]);

			_alarms.Wait();

			var alarms = _alarms.Result;
			if (alarms == null)
				throw new GenIfException("No alarms found.");

			if (alarms.Count == 0)
				return new GQIPage(new GQIRow[0]);

			var rows = new List<GQIRow>(alarms.Count);

			foreach (var alarm in alarms)
			{
				var cells = new[]
				{
					new GQICell {Value= $"{alarm.DataMinerID}/{alarm.AlarmID }"}, // IDColumn
					new GQICell {Value= alarm.ElementName }, // ElementColumn,
					new GQICell {Value= alarm.ParameterName }, // ParameterColumn,
					new GQICell {Value= alarm.DisplayValue }, // ValueColumn,
					new GQICell {Value= alarm.CreationTime.ToUniversalTime() }, // TimeColumn,
					new GQICell {Value= alarm.Severity }, // SeverityColumn,
					new GQICell {Value = alarm.Owner}, // OwnerColumn
				};

				rows.Add(new GQIRow(cells));
			}

			return new GQIPage(rows.ToArray()) { HasNextPage = false };
		}
	}
}