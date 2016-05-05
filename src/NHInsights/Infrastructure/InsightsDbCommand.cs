namespace NHInsights.Infrastructure
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Diagnostics.CodeAnalysis;
    using System.Reflection;
    using System.Reflection.Emit;
    using System.Text;
    using Microsoft.ApplicationInsights;
    using Microsoft.ApplicationInsights.DataContracts;
    using Microsoft.ApplicationInsights.Extensibility;
    using NHInsights.Infrastructure;
    
    /// <summary>
    /// A general implementation of <see cref="IDbCommand"/> that uses an <see cref="IDbProfiler"/>
    /// to collect profiling information.
    /// </summary>
    public class InsightsDbCommand : DbCommand, ICloneable
    {
        /// <summary>
        /// The bind by name cache.
        /// </summary>
        private static Link<Type, Action<IDbCommand, bool>> bindByNameCache;

        /// <summary>
        /// The command.
        /// </summary>
        private DbCommand _command;

        /// <summary>
        /// The connection.
        /// </summary>
        private DbConnection _connection;

        /// <summary>
        /// The telemetryClient.
        /// </summary>
        private static TelemetryClient telemetryClient = new TelemetryClient(TelemetryConfiguration.Active);

        /// <summary>
        /// The transaction.
        /// </summary>
        private DbTransaction _transaction;

        /// <summary>
        /// bind by name.
        /// </summary>
        private bool _bindByName;

        /// <summary>
        /// Gets or sets a value indicating whether or not to bind by name.
        /// If the underlying command supports BindByName, this sets/clears the underlying
        /// implementation accordingly. This is required to support OracleCommand from dapper-dot-net
        /// </summary>
        public bool BindByName
        {
            get
            {
                return _bindByName;
            }

            set
            {
                if (_bindByName != value)
                {
                    if (_command != null)
                    {
                        var inner = GetBindByName(_command.GetType());
                        if (inner != null) inner(_command, value);
                    }

                    _bindByName = value;
                }
            }
        }

        /// <summary>
        /// get the binding name.
        /// </summary>
        /// <param name="commandType">The command type.</param>
        /// <returns>The <see cref="Action"/>.</returns>
        private static Action<IDbCommand, bool> GetBindByName(Type commandType)
        {
            if (commandType == null) return null; // GIGO
            Action<IDbCommand, bool> action;
            if (Link<Type, Action<IDbCommand, bool>>.TryGet(bindByNameCache, commandType, out action))
            {
                return action;
            }

            var prop = commandType.GetProperty("BindByName", BindingFlags.Public | BindingFlags.Instance);
            action = null;
            ParameterInfo[] indexers;
            MethodInfo setter;
            if (prop != null && prop.CanWrite && prop.PropertyType == typeof(bool)
                && ((indexers = prop.GetIndexParameters()) == null || indexers.Length == 0)
                && (setter = prop.GetSetMethod()) != null)
            {
                var method = new DynamicMethod(commandType.Name + "_BindByName", null, new[] { typeof(IDbCommand), typeof(bool) });
                var il = method.GetILGenerator();
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Castclass, commandType);
                il.Emit(OpCodes.Ldarg_1);
                il.EmitCall(OpCodes.Callvirt, setter, null);
                il.Emit(OpCodes.Ret);
                action = (Action<IDbCommand, bool>)method.CreateDelegate(typeof(Action<IDbCommand, bool>));
            }

            // cache it            
            Link<Type, Action<IDbCommand, bool>>.TryAdd(ref bindByNameCache, commandType, ref action);
            return action;
        }

        /// <summary>
        /// Initialises a new instance of the <see cref="SimpleProfiledCommand"/> class. 
        /// Creates a new wrapped command
        /// </summary>
        /// <param name="command">The wrapped command</param>
        /// <param name="connection">The wrapped connection the command is attached to</param>
        /// <param name="telemetryClient">The telemetryClient to use</param>
        public InsightsDbCommand(DbCommand command, DbConnection connection)
        {
            if (command == null) throw new ArgumentNullException("command");

            _command = command;
            _connection = connection;
        }

        /// <summary>
        /// prepare the command.
        /// </summary>
        public override void Prepare()
        {
            _command.Prepare();
        }

        /// <summary>
        /// cancel the command.
        /// </summary>
        public override void Cancel()
        {
            _command.Cancel();
        }

        /// <summary>
        /// Gets or sets a value indicating whether the command is design time visible.
        /// </summary>
        public override bool DesignTimeVisible
        {
            get { return _command.DesignTimeVisible; }
            set { _command.DesignTimeVisible = value; }
        }

        /// <summary>
        /// create a new parameter.
        /// </summary>
        /// <returns>The <see cref="IDbDataParameter"/>.</returns>
        protected override DbParameter CreateDbParameter()
        {
            return _command.CreateParameter();
        }

        /// <summary>
        /// execute a non query.
        /// </summary>
        /// <returns>The <see cref="int"/>.</returns>
        public override int ExecuteNonQuery()
        {
            return TrackDependency("NonQuery", _command.ExecuteNonQuery);
        }

        /// <summary>
        /// execute the reader.
        /// </summary>
        /// <param name="behavior">The <c>behavior</c>.</param>
        /// <returns>the active reader.</returns>
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return TrackDependency(
                "Reader", () => _command.ExecuteReader(behavior));
        }

        /// <summary>
        /// execute and return a scalar.
        /// </summary>
        /// <returns>the scalar value.</returns>
        public override object ExecuteScalar()
        {
            return TrackDependency("Scalar", () => _command.ExecuteScalar());
        }

        /// <summary>
        /// profile with results.
        /// </summary>
        /// <param name="type">The type of execution.</param>
        /// <param name="func">a function to execute against against the profile result</param>
        /// <typeparam name="TResult">the type of result to return.</typeparam>
        private TResult TrackDependency<TResult>(string type, Func<TResult> func)
        {
            if (telemetryClient == null || !telemetryClient.IsEnabled())
            {
                return func();
            }

            var success = false;
            var startTime = DateTime.UtcNow;
            var timer = System.Diagnostics.Stopwatch.StartNew();
            TResult result;
            try
            {
                result = func();
                success = true;
            }
            finally
            {
                timer.Stop();
                var dependencyType = _command.GetType().FullName;
                var dependencyTelemetry = new DependencyTelemetry(dependencyType, type, startTime, timer.Elapsed, success);
                dependencyTelemetry.DependencyTypeName = dependencyType;

                StringBuilder builder = new StringBuilder();
                builder.Append(_command.CommandText);
                builder.AppendLine();
                builder.AppendLine();
                foreach (DbParameter parameter in _command.Parameters)
                { 
                    builder.AppendLine(string.Format("{0} = {1}", parameter.ParameterName, parameter.Value.ToString()));
                }

                dependencyTelemetry.Properties.Add(new KeyValuePair<string, string>("commandText", builder.ToString()));
                telemetryClient.TrackDependency(dependencyTelemetry);
            }

            return result;
        }

        /// <summary>
        /// Gets or sets the connection.
        /// </summary>
        protected override DbConnection DbConnection
        {
            get
            {
                return _connection;
            }
            set
            {
                _connection = value;
                _command.Connection = value;
            }
        }

        /// <summary>
        /// Gets or sets the transaction.
        /// </summary>
        protected override DbTransaction DbTransaction
        {
            get
            {
                return _transaction;
            }
            set
            {
                _transaction = value;
                _command.Transaction = value;
            }
        }

        /// <summary>
        /// Gets or sets the command text.
        /// </summary>
        [SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities", Justification = "Handled elsewhere.")]
        public override string CommandText
        {
            get { return _command.CommandText; }
            set { _command.CommandText = value; }
        }

        /// <summary>
        /// Gets or sets the command timeout.
        /// </summary>
        public override int CommandTimeout
        {
            get { return _command.CommandTimeout; }
            set { _command.CommandTimeout = value; }
        }

        /// <summary>
        /// Gets or sets the command type.
        /// </summary>
        public override CommandType CommandType
        {
            get { return _command.CommandType; }
            set { _command.CommandType = value; }
        }

        /// <summary>
        /// Gets the parameters.
        /// </summary>
        protected override DbParameterCollection DbParameterCollection
        {
            get { return _command.Parameters; }
        }

        /// <summary>
        /// Gets or sets the updated row source.
        /// </summary>
        public override UpdateRowSource UpdatedRowSource
        {
            get { return _command.UpdatedRowSource; }
            set { _command.UpdatedRowSource = value; }
        }

        /// <summary>
        /// dispose the command / connection and telemetryClient.
        /// </summary>
        /// <param name="disposing">false if the dispose is called from a <c>finalizer</c></param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && _command != null) _command.Dispose();

            _command = null;
            _connection = null;
        }

        /// <summary>
        /// clone the command.
        /// </summary>
        /// <returns>The <see cref="object"/>.</returns>
        public object Clone()
        {
            var tail = _command as ICloneable;
            if (tail == null)
                throw new NotSupportedException("Underlying " + _command.GetType().Name + " is not cloneable.");

            return new InsightsDbCommand((DbCommand)tail.Clone(), _connection);
        }
    }

}